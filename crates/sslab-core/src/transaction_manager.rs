use std::{
    collections::{HashMap, VecDeque},
    sync::Arc, cmp::max
};
use mysten_metrics::{monitored_scope, spawn_monitored_task, spawn_logged_monitored_task};
use narwhal_types::ConditionalBroadcastReceiver;
use sui_types::digests::TransactionDigest;
use crate::{types::ExecutableEthereumTransaction, execution_storage::{MemoryStorage, ExecutionResult, ExecutionBackend}};
use parking_lot::RwLock;
use tokio::task::JoinHandle;
use tokio::sync::mpsc::{UnboundedSender, Receiver, Sender, channel};
use tracing::trace;
use sui_types::error::SuiResult;


/// Minimum capacity of HashMaps used in TransactionManager.
pub(crate) const MIN_HASHMAP_CAPACITY: usize = 1000;


#[derive(Clone, Debug)]
struct PendingCertificate {
    // Certified transaction to be executed.
    pub certificate: ExecutableEthereumTransaction,
    pub next: TransactionDigest,
}

impl PendingCertificate {
    fn digest(&self) -> &TransactionDigest {
        self.certificate.digest()
    }

    fn next(&self) -> &TransactionDigest {
        &self.next
    }

    fn has_next(&self) -> bool {
        self.next != TransactionDigest::default()
    }
}

#[async_trait::async_trait]
pub(crate) trait TxManager<T> {
    async fn enqueue(&self, transactions: Vec<T>) -> SuiResult<()>;
    async fn commit(&self, certificate: &TransactionDigest, execution_result: &ExecutionResult);
}

pub struct SimpleTransactionManager {
    execution_store: Arc<RwLock<MemoryStorage>>,
    rx_consensus_certificate: Receiver<Vec<ExecutableEthereumTransaction>>,
    tx_ready_certificate: UnboundedSender<ExecutableEthereumTransaction>,
    rx_commit_notification: Receiver<(TransactionDigest, ExecutionResult)>,
    inner: RwLock<Inner>,
}

#[async_trait::async_trait]
impl TxManager<ExecutableEthereumTransaction> for SimpleTransactionManager {
    async fn enqueue(&self, transactions: Vec<ExecutableEthereumTransaction>) -> SuiResult<()> {
        self._enqueue(transactions).await
    }

    async fn commit(&self, digest: &TransactionDigest, execution_result: &ExecutionResult) {
        let mut inner = self.inner.write();
        let _scope = monitored_scope("TransactionManager::commit::wlock");

        let Some(executed_cert) = inner.executing_certificates.remove(digest) else {
            trace!("{:?} not found in executing certificates, likely because it is a system transaction", digest);
            return;
        };
        
        self.execution_store.write().apply_all_effects(digest, execution_result);
        
        let next_cert = match executed_cert.has_next() {
            true => inner.pending_certificates.remove(executed_cert.next()).expect("next certificate must exist in pending queue"),
            false => {
                if inner.pending_queue.is_empty() {
                    return  // unlikely happens. empty queue means that consensus progresses slower than execution.
                } else {
                    let cert = inner.pending_queue.pop_front().unwrap();
                    inner.pending_certificates.remove(&cert).expect("pending certificate in queue must exist")
                }
            }
        };

        let cert = next_cert.certificate.clone();
        inner.executing_certificates.insert(cert.digest().clone(), next_cert);
        let _ = self
            .tx_ready_certificate
            .send(cert);

        // self.metrics
        // .transaction_manager_num_executing_certificates
        // .set(inner.executing_certificates.len() as i64);

        inner.maybe_shrink_capacity();
        
    }
}

impl SimpleTransactionManager {

    pub fn spawn(
        execution_store: Arc<RwLock<MemoryStorage>>,
        tx_consensus_confirmation: Sender<Vec<TransactionDigest>>,
        rx_consensus_certificate: Receiver<Vec<ExecutableEthereumTransaction>>,
        tx_ready_certificate: UnboundedSender<ExecutableEthereumTransaction>,
        rx_commit_notification: Receiver<(TransactionDigest, ExecutionResult)>,
        rv_shutdown: ConditionalBroadcastReceiver,
    ) -> JoinHandle<()>{
        spawn_logged_monitored_task!(
            Self {
                execution_store,
                inner: RwLock::new(Inner::new()),
                rx_consensus_certificate,
                tx_ready_certificate,
                rx_commit_notification
            }.run(),
            "transaction_manager::run()"
        )
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some((digest, execution_result)) = self.rx_commit_notification.recv() => {
                    let _ = self.commit(&digest, &execution_result);
                }

                Some(transactions) = self.rx_consensus_certificate.recv() => {
                    let _ = self.enqueue(transactions).await;
                }
            }
        }
    }

    async fn _enqueue(&self, transactions: Vec<ExecutableEthereumTransaction>) -> SuiResult<()>{

        // TODO: filter out already executed transactionss
        // let transactions: Vec<_> = transactions
        // .into_iter()
        // .filter(|tx| {
        //     let digest = *tx.digest();
        //     // skip already executed txes
        //     if self
        //         .authority_store
        //         .is_tx_already_executed(&digest)
        //         .expect("Failed to check if tx is already executed")
        //     {
        //         // also ensure the transaction will not be retried after restart.
        //         let _ = epoch_store.remove_pending_execution(&digest);
        //         self.metrics
        //             .transaction_manager_num_enqueued_certificates
        //             .with_label_values(&["already_executed"])
        //             .inc();
        //         false
        //     } else {
        //         true
        //     }
        // })
        // .collect();


        // 3. make given consunsus output to pending transactions

        // Internal lock is held only for updating the internal state.
        let mut inner = self.inner.write();
        let _scope = monitored_scope("TransactionManager::enqueue::wlock");

        let pending = transactions.iter().enumerate().map(|(idx, tx)| {
            if transactions.len() > idx + 1 {
                PendingCertificate {certificate: tx.clone(), next: *transactions[idx+1].digest()}
            } else {
                PendingCertificate {certificate: tx.clone(), next: TransactionDigest::default()}
            }
        });

        for pending_cert in pending {
            let digest = *pending_cert.certificate.digest();

            // skip already pending txes
            if inner.pending_certificates.contains_key(&digest) {
                // self.metrics
                //     .transaction_manager_num_enqueued_certificates
                //     .with_label_values(&["already_pending"])
                //     .inc();
                continue;
            }
            // skip already executing txes
            if inner.executing_certificates.contains_key(&digest) {
                // self.metrics
                //     .transaction_manager_num_enqueued_certificates
                //     .with_label_values(&["already_executing"])
                //     .inc();
                continue;
            }
            // TODO: skip already executed txes
            // if self.authority_store.is_tx_already_executed(&digest)? {
                // self.metrics
                //     .transaction_manager_num_enqueued_certificates
                //     .with_label_values(&["already_executed"])
                //     .inc();
            //     continue;
            // }

            // Ready transactions can start to execute.
            // self.metrics
            //     .transaction_manager_num_enqueued_certificates
            //     .with_label_values(&["ready"])
            //     .inc();
            // Send to execution driver for execution.
            self.certificate_ready(&mut inner, pending_cert); 
        }

        // self.metrics
        //     .transaction_manager_num_pending_transactions
        //     .set(inner.pending_transactions.len() as i64);

        inner.maybe_reserve_capacity();

        Ok(())
    }

    /// Sends the ready certificate for execution.
    fn certificate_ready(&self, inner: &mut Inner, pending_certificate: PendingCertificate) {
        let cert = pending_certificate.certificate.clone();
        trace!(tx_digest = ?cert.digest(), "transaction ready");
        
        // If there are no pending transactions and no current executing transaction, send to execution driver directly.
        if inner.pending_certificates.is_empty() && inner.executing_certificates.is_empty() {

            // Record as an executing certificate.
            inner.executing_certificates.insert(cert.digest().clone(), pending_certificate);

            let _ = self
            .tx_ready_certificate
            .send(cert);
        // self.metrics.transaction_manager_num_ready.inc();
        // self.metrics.execution_driver_dispatch_queue.inc();
        } else {
            assert!(inner
                .pending_certificates
                .insert(cert.digest().clone(), pending_certificate)
                .is_none());
            inner.pending_queue.push_back(*cert.digest());
        }
    }

    
}



struct Inner {
    // Current epoch of TransactionManager.
    // epoch: EpochId,

    // A transaction enqueued to TransactionManager must be in either pending_transactions or
    // executing_certificates.
    pending_queue: VecDeque<TransactionDigest>,
    // Maps transaction digests to their content and missing input objects.
    pending_certificates: HashMap<TransactionDigest, PendingCertificate>,
    // Maps executing transaction digests to their acquired input object locks.
    executing_certificates: HashMap<TransactionDigest, PendingCertificate>,
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
