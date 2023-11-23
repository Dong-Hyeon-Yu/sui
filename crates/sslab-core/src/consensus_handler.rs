use futures::stream::FuturesUnordered;
use mysten_metrics::spawn_logged_monitored_task;
use async_trait::async_trait;
use fastcrypto::hash::Hash as _Hash;
use narwhal_executor::ExecutionState;
use narwhal_types::{BatchAPI, CertificateAPI, ConsensusOutput, HeaderAPI};
use tokio::{sync::mpsc::Sender, task::JoinHandle};
use tracing::{info, instrument};
use crate::{types::{ExecutableEthereumBatch, EthereumTransaction, ExecutableConsensusOutput}, executor::ExecutionComponent};
use core::panic;
use std::sync::Arc;

#[allow(dead_code)]
pub struct SimpleConsensusHandler {
    tx_consensus_certificate: Sender<ExecutableConsensusOutput>,
    // tx_shutdown: Option<PreSubscribedBroadcastSender>,
    handles: FuturesUnordered<JoinHandle<()>>,
}

impl SimpleConsensusHandler {
  

    pub fn new<Executor>(
        mut executor: Executor,
        tx_consensus_certificate: Sender<ExecutableConsensusOutput>,
    ) -> Self 
        where Executor: ExecutionComponent + Send + Sync + 'static
    {   
        let handles = FuturesUnordered::new();

        handles.push(
            spawn_logged_monitored_task!(
                async move{ executor.run().await; },
                "executor.run()"
            )
        );

        Self {
            tx_consensus_certificate,
            // tx_shutdown: Some(tx_shutdown),
            handles,
        }
    }

    // pub async fn shutdown(&mut self) {
    //     // send the shutdown signal to the node
    //     let now = Instant::now();
    //     info!("Sending shutdown message to primary node");

    //     if let Some(tx_shutdown) = self.tx_shutdown.as_ref() {
    //         tx_shutdown
    //             .send()
    //             .expect("Couldn't send the shutdown signal to downstream components");
    //         self.tx_shutdown = None;
    //     }

        

    //     // Now wait until handles have been completed
    //     try_join_all(&mut self.handles).await.unwrap();

    //     info!(
    //         "Narwhal primary shutdown is complete - took {} seconds",
    //         now.elapsed().as_secs_f64()
    //     );
    // }
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

        // NOTE: This log entry is used to compute performance.
        info!("Received consensus_output has {} batches at subdag_index {}.", consensus_output.sub_dag.num_batches(), consensus_output.sub_dag.sub_dag_index);

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

        let executable_consensus_output = ExecutableConsensusOutput::new(ethereum_batches.clone(), &consensus_output);

        if !ethereum_batches.is_empty() {
            let _ = self.tx_consensus_certificate
                .send(executable_consensus_output)
                .await;
        }
    }

    async fn last_executed_sub_dag_index(&self) -> u64 {
        0
    }

}