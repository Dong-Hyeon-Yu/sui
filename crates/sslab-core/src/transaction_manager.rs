use std::{
    collections::{HashMap, VecDeque},
    sync::Arc, cmp::max
};
use mysten_metrics::{monitored_scope, spawn_logged_monitored_task};
use narwhal_types::{ConditionalBroadcastReceiver, BatchDigest};
use crate::{types::ExecutableEthereumBatch, execution_storage::{MemoryStorage, ExecutionResult, ExecutionBackend}};
use parking_lot::RwLock;
use tokio::task::JoinHandle;
use tokio::sync::mpsc::{UnboundedSender, Receiver, Sender};
use tracing::{instrument, debug, warn, trace};
use sui_types::error::SuiResult;


/// Minimum capacity of HashMaps used in TransactionManager.
pub(crate) const MIN_HASHMAP_CAPACITY: usize = 1000;


#[async_trait::async_trait]
pub(crate) trait TxManager<T> {
    
    async fn enqueue(&self, transactions: Vec<T>) -> SuiResult<()>;

    async fn commit(&self, certificate: &BatchDigest, execution_result: &ExecutionResult);
}

pub struct SimpleTransactionManager {
    execution_store: Arc<RwLock<MemoryStorage>>,
    tx_execution_confirmation: Sender<BatchDigest>,
    rx_consensus_certificate: Receiver<Vec<ExecutableEthereumBatch>>,
    tx_ready_certificate: UnboundedSender<ExecutableEthereumBatch>,
    rx_commit_notification: Receiver<(BatchDigest, ExecutionResult)>,
    rx_shutdown: ConditionalBroadcastReceiver,
    inner: RwLock<Inner>,
}

#[async_trait::async_trait]
impl TxManager<ExecutableEthereumBatch> for SimpleTransactionManager {
    async fn enqueue(&self, transactions: Vec<ExecutableEthereumBatch>) -> SuiResult<()> {
        self._enqueue(transactions).await
    }

    #[instrument(level = "trace", skip_all)] 
    async fn commit(&self, digest: &BatchDigest, execution_result: &ExecutionResult) {
        let mut inner = self.inner.write();
        let _scope = monitored_scope("TransactionManager::commit::wlock");

        let Some(_) = inner.executing_certificates.remove(digest) else {
            warn!("{:?} not found in executing certificates", digest);
            return;
        };
        
        self.execution_store.write().apply_all_effects(digest, execution_result);


        if inner.pending_queue.is_empty() {
            return  // unlikely happens. empty queue means that consensus progresses slower than execution.
        } else {
            let next_cert = inner.pending_queue.pop_front().unwrap();
            let next_batch = inner.pending_certificates.remove(&next_cert).expect("pending certificate in queue must exist");

            assert!(inner.executing_certificates.insert(next_cert.clone(), next_batch.clone()).is_none());
            let _ = self
                .tx_ready_certificate
                .send(next_batch);

            inner.maybe_shrink_capacity();
        };
    }
}

impl SimpleTransactionManager {

    pub fn spawn(
        execution_store: Arc<RwLock<MemoryStorage>>,
        tx_execution_confirmation: Sender<BatchDigest>,
        rx_consensus_certificate: Receiver<Vec<ExecutableEthereumBatch>>,
        tx_ready_certificate: UnboundedSender<ExecutableEthereumBatch>,
        rx_commit_notification: Receiver<(BatchDigest, ExecutionResult)>,
        rx_shutdown: ConditionalBroadcastReceiver,
    ) -> JoinHandle<()>{
        spawn_logged_monitored_task!(
            Self {
                execution_store,
                inner: RwLock::new(Inner::new()),
                tx_execution_confirmation,
                rx_consensus_certificate,
                tx_ready_certificate,
                rx_commit_notification,
                rx_shutdown
            }.run(),
            "transaction_manager::run()"
        )
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn run(&mut self) {
        loop {
            tokio::select! {

                biased;

                Some((digest, execution_result)) = self.rx_commit_notification.recv() => {
                    let _ = self.commit(&digest, &execution_result).await;
                    let _ = self.tx_execution_confirmation.send(digest).await;
                }

                Some(ethereum_batches) = self.rx_consensus_certificate.recv() => {
                    debug!("Received {} batches from consensus", ethereum_batches.len());
                    let _ = self.enqueue(ethereum_batches).await;
                }

                _ = self.rx_shutdown.receiver.recv() => {
                    break;
                }
            }
        }
    }

    #[instrument(level = "trace", skip_all)]
    async fn _enqueue(&self, transactions: Vec<ExecutableEthereumBatch>) -> SuiResult<()>{

        // Internal lock is held only for updating the internal state.
        let mut inner = self.inner.write();

        for tx in transactions {
            let digest = tx.digest();

            // skip already pending txes
            if inner.pending_certificates.contains_key(digest) {
                continue;
            }
            // skip already executing txes
            if inner.executing_certificates.contains_key(digest) {
                continue;
            }
            // skip already executed txes
            if self.execution_store.read().is_tx_already_executed(digest)? {
                continue;
            }

            // Ready transactions can start to execute.
            // Send to execution driver for execution.
            self.certificate_ready(&mut inner, tx); 
        }

        inner.maybe_reserve_capacity();

        Ok(())
    }

    /// Sends the ready certificate for execution.
    #[instrument(level = "trace", skip_all)]
    fn certificate_ready(&self, inner: &mut Inner, pending_certificate: ExecutableEthereumBatch) {
        let cert = pending_certificate.digest();
        trace!(tx_digest = ?cert, "transaction ready");

        // If no current executing transaction, send to execution driver directly.
        if inner.pending_certificates.is_empty() && inner.executing_certificates.is_empty() {

            // Record as an executing certificate.
            inner.executing_certificates.insert(*cert, pending_certificate.clone());

            let _ = self
            .tx_ready_certificate
            .send(pending_certificate);
        } else {
            assert!(inner
                .pending_certificates
                .insert(*cert, pending_certificate.clone())
                .is_none());
            inner.pending_queue.push_back(*cert);
        }
    }
    
}



struct Inner {
    // Current epoch of TransactionManager.
    // epoch: EpochId,

    // A transaction enqueued to TransactionManager must be in either pending_transactions or
    // executing_certificates.
    pending_queue: VecDeque<BatchDigest>,
    // Maps transaction digests to their content and missing input objects.
    pending_certificates: HashMap<BatchDigest, ExecutableEthereumBatch>,
    // Maps executing transaction digests to their acquired input object locks.
    executing_certificates: HashMap<BatchDigest, ExecutableEthereumBatch>,
}

impl Inner {
    fn new() -> Inner {
        Inner {
            pending_queue: VecDeque::new(),
            pending_certificates: HashMap::with_capacity(MIN_HASHMAP_CAPACITY),
            executing_certificates: HashMap::with_capacity(MIN_HASHMAP_CAPACITY),
        }
    }

    fn maybe_reserve_capacity(&mut self) {
        self.pending_certificates.maybe_reserve_capacity();
        self.executing_certificates.maybe_reserve_capacity();
    }

    /// After reaching 1/4 load in hashmaps, decrease capacity to increase load to 1/2.
    fn maybe_shrink_capacity(&mut self) {
        self.pending_certificates.maybe_shrink_capacity();
        self.executing_certificates.maybe_shrink_capacity();
    }
}

trait ResizableHashMap<K, V> {
    fn maybe_reserve_capacity(&mut self);
    fn maybe_shrink_capacity(&mut self);
}

impl<K, V> ResizableHashMap<K, V> for HashMap<K, V>
where
    K: std::cmp::Eq + std::hash::Hash,
{
    /// After reaching 3/4 load in hashmaps, increase capacity to decrease load to 1/2.
    fn maybe_reserve_capacity(&mut self) {
        if self.len() > self.capacity() * 3 / 4 {
            self.reserve(self.capacity() / 2);
        }
    }

    /// After reaching 1/4 load in hashmaps, decrease capacity to increase load to 1/2.
    fn maybe_shrink_capacity(&mut self) {
        if self.len() > MIN_HASHMAP_CAPACITY && self.len() < self.capacity() / 4 {
            self.shrink_to(max(self.capacity() / 2, MIN_HASHMAP_CAPACITY))
        }
    }
}
