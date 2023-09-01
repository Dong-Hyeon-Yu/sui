use futures::{future::try_join_all, stream::FuturesUnordered};
use mysten_metrics::spawn_logged_monitored_task;
use async_trait::async_trait;
use fastcrypto::hash::Hash as _Hash;
use narwhal_executor::ExecutionState;
use narwhal_types::{BatchAPI, CertificateAPI, ConsensusOutput, HeaderAPI, PreSubscribedBroadcastSender};
use parking_lot::RwLock;
use tokio::{sync::mpsc::Sender, task::JoinHandle};
use tracing::{info, instrument};
use crate::{types::{ExecutableEthereumBatch, EthereumTransaction}, transaction_manager::SimpleTransactionManager, execution_storage::MemoryStorage, execution_driver::execution_process};
use core::panic;
use std::{sync::Arc, time::Instant};

pub struct SimpleConsensusHandler {
    tx_consensus_certificate: Sender<Vec<ExecutableEthereumBatch>>,
    tx_shutdown: Option<PreSubscribedBroadcastSender>,
    handles: FuturesUnordered<JoinHandle<()>>,
}

impl SimpleConsensusHandler {

    // ConsensusHandler has two components : SimpleTransactionManager and ExecutionDriver
    const NUM_SHUTDOWN_RECEIVERS: u64 = 3;  

    pub fn new(
        execution_store: Arc<RwLock<MemoryStorage>>, //TODO: make this field Arc, and remove Inner
    ) -> Self {
        let (tx_consensus_certificate, rx_consensus_certificate) = tokio::sync::mpsc::channel(100);
        let (tx_ready_certificate, rx_ready_certificate) = tokio::sync::mpsc::unbounded_channel();
        let (tx_commit_notification, rx_commit_notification) = tokio::sync::mpsc::channel(100);
        // The channel returning the result for each transaction's execution.
        let (tx_execution_confirmation, rx_execution_confirmation) = tokio::sync::mpsc::channel(100);

        let mut tx_shutdown = PreSubscribedBroadcastSender::new(Self::NUM_SHUTDOWN_RECEIVERS);
        
        let handles = FuturesUnordered::new();

        let mut rx_shutdown = tx_shutdown.subscribe();
        handles.push(spawn_logged_monitored_task!(async move {
            let mut rx = rx_execution_confirmation;

            loop {
                tokio::select! {
                    Some(digest) = rx.recv() => {
                        // NOTE: This log entry is used to compute performance.
                        info!("Executed Batch -> {:?}", digest);
                    }
                    _ = rx_shutdown.receiver.recv() => {
                        info!("Shutdown signal received. Exiting executor ...");
                        return;
                    }
                }
            }}, 
            "confirmation_loop")
        );


        let database = execution_store.clone();
        let rx_shutdown = tx_shutdown.subscribe();
        handles.push(
            spawn_logged_monitored_task!(
                execution_process(
                    database,
                    rx_ready_certificate,
                    tx_commit_notification,
                    rx_shutdown
                ),
                "ExecutionDriver::loop"
            )
        );

        handles.push(
            SimpleTransactionManager::spawn(
                execution_store.clone(),
                tx_execution_confirmation,
                rx_consensus_certificate,
                tx_ready_certificate,
                rx_commit_notification,
                tx_shutdown.subscribe(),
            )
        );

        Self {
            tx_consensus_certificate,
            tx_shutdown: Some(tx_shutdown),
            handles,
        }
    }

    pub async fn shutdown(&mut self) {
        // send the shutdown signal to the node
        let now = Instant::now();
        info!("Sending shutdown message to primary node");

        if let Some(tx_shutdown) = self.tx_shutdown.as_ref() {
            tx_shutdown
                .send()
                .expect("Couldn't send the shutdown signal to downstream components");
            self.tx_shutdown = None;
        }

        

        // Now wait until handles have been completed
        try_join_all(&mut self.handles).await.unwrap();

        info!(
            "Narwhal primary shutdown is complete - took {} seconds",
            now.elapsed().as_secs_f64()
        );
    }
}

#[async_trait]
impl ExecutionState for SimpleConsensusHandler {

    /// This function will be called by Narwhal, after Narwhal sequenced this certificate.
    #[instrument(level = "trace", skip_all)]
    async fn handle_consensus_output(&self, consensus_output: ConsensusOutput) {
        let round = consensus_output.sub_dag.leader_round();

        /* (serialized, transaction, output_cert) */
        let mut ethereum_batches : Vec<ExecutableEthereumBatch> = vec![];
        let timestamp = consensus_output.sub_dag.commit_timestamp();

        info!(
            "Received consensus output {:?} at leader round {}, subdag index {}, timestamp {} ",
            consensus_output.digest(),
            round,
            consensus_output.sub_dag.sub_dag_index,
            timestamp.clone(),
        );

        for (cert, batches) in consensus_output
            .sub_dag
            .certificates
            .iter()
            .zip(consensus_output.batches.iter())
        {
            assert_eq!(cert.header().payload().len(), batches.len());
            
            let output_cert = Arc::new(cert.clone());
            for batch in batches {
                assert!(output_cert.header().payload().contains_key(&batch.digest()));

                if batch.transactions().is_empty() {
                    continue;
                }

                let mut _batch_tx: Vec<EthereumTransaction> = vec![];
                for serialized_transaction in batch.transactions() {

                    let transaction = match EthereumTransaction::decode(serialized_transaction) {
                        Ok(transaction) => transaction,
                        Err(err) => {
                            // This should have been prevented by Narwhal batch verification.
                            panic!(
                                "Unexpected malformed transaction (failed to deserialize): {}\nCertificate={:?} BatchDigest={:?} Transaction={:?}",
                                err, output_cert, batch.digest(), serialized_transaction
                            );
                        }
                    };
                    _batch_tx.push(transaction);
                }

                if !_batch_tx.is_empty() {
                    ethereum_batches.push(ExecutableEthereumBatch::new(_batch_tx, batch.digest()));
                }
            }
        }

        if !ethereum_batches.is_empty() {
            let _ = self.tx_consensus_certificate
                .send(ethereum_batches)
                .await;
        }
    }

    async fn last_executed_sub_dag_index(&self) -> u64 {
        0
    }

}