use async_trait::async_trait;
use core::panic;
use fastcrypto::hash::Hash as _Hash;
use futures::stream::FuturesUnordered;
use mysten_metrics::spawn_logged_monitored_task;
use narwhal_executor::ExecutionState;
use narwhal_types::{BatchAPI, BatchDigest, CertificateAPI, ConsensusOutput, HeaderAPI};
use rayon::prelude::*;
use sslab_execution::{
    executor::ExecutionComponent,
    types::{EthereumTransactable, ExecutableConsensusOutput, ExecutableEthereumBatch},
};
use tokio::{sync::mpsc::Sender, task::JoinHandle};
use tracing::{info, instrument, warn};

#[allow(dead_code)]
pub struct SimpleConsensusHandler<T: EthereumTransactable + Clone> {
    tx_consensus_certificate: Sender<ExecutableConsensusOutput<T>>,
    // tx_shutdown: Option<PreSubscribedBroadcastSender>,
    handles: FuturesUnordered<JoinHandle<()>>,
}

impl<T: EthereumTransactable + Clone> SimpleConsensusHandler<T> {
    pub fn new<Executor>(
        mut executor: Executor,
        tx_consensus_certificate: Sender<ExecutableConsensusOutput<T>>,
    ) -> Self
    where
        Executor: ExecutionComponent + Send + Sync + 'static,
    {
        let handles = FuturesUnordered::new();

        handles.push(spawn_logged_monitored_task!(
            async move {
                executor.run().await;
            },
            "executor.run()"
        ));

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
impl<T: EthereumTransactable + Clone + Send + 'static> ExecutionState
    for SimpleConsensusHandler<T>
{
    /// This function will be called by Narwhal, after Narwhal sequenced this certificate.
    #[instrument(level = "trace", skip_all)]
    async fn handle_consensus_output(&self, consensus_output: ConsensusOutput) {
        info!(
            "Received consensus output {:?} at leader round {}, subdag index {}, timestamp {} ",
            consensus_output.digest(),
            consensus_output.sub_dag.leader_round(),
            consensus_output.sub_dag.sub_dag_index,
            consensus_output.sub_dag.commit_timestamp(),
        );

        cfg_if::cfg_if! {
            if #[cfg(feature = "benchmark")] {
                // NOTE: This log entry is used to compute performance.
                consensus_output.sub_dag.certificates.iter().for_each(|cert| {
                    cert.header().payload().keys().for_each(|digest| info!("Consensus handler received a batch -> {:?}", digest));
                });

                // NOTE: This log entry is used to compute performance.
                info!("Received consensus_output has {} batches at subdag_index {}.", consensus_output.sub_dag.num_batches(), consensus_output.sub_dag.sub_dag_index);
            }
        }

        /* (serialized, transaction, output_cert) */
        let mut ethereum_batches: Vec<ExecutableEthereumBatch<T>> = vec![];

        for (cert, batches) in consensus_output
            .sub_dag
            .certificates
            .iter()
            .zip(consensus_output.batches.iter())
        {
            assert_eq!(cert.header().payload().len(), batches.len());

            for batch in batches {
                assert!(cert.header().payload().contains_key(&batch.digest()));

                if batch.transactions().is_empty() {
                    continue;
                }

                let _batch = std::sync::Arc::new(batch.clone());
                let digest = _batch.digest();
                let _digest = digest.clone();

                let _batch_tx = tokio::task::spawn_blocking(move || {
                    _batch
                        .transactions()
                        .par_iter()
                        .map(|serialized_transaction| {
                            decode_transaction(serialized_transaction, _digest)
                        })
                        .collect::<Vec<_>>()
                })
                .await
                .expect("Failed to spawn a thread for decoding transactions.");

                if !_batch_tx.is_empty() {
                    ethereum_batches.push(ExecutableEthereumBatch::new(_batch_tx, digest));
                } else {
                    warn!("Received an empty decoded batch at subdag_index {}. This couldn't possible.", consensus_output.sub_dag.sub_dag_index)
                }
            }
        }

        let executable_consensus_output =
            ExecutableConsensusOutput::new(ethereum_batches, &consensus_output);

        if !executable_consensus_output.data().is_empty() {
            let _ = self
                .tx_consensus_certificate
                .send(executable_consensus_output)
                .await;
        }
    }

    async fn last_executed_sub_dag_index(&self) -> u64 {
        0
    }
}

pub fn decode_transaction<T: EthereumTransactable>(
    serialized_transaction: &Vec<u8>,
    batch_digest: BatchDigest,
) -> T {
    match T::from_json(serialized_transaction) {
        Ok(transaction) => transaction,
        Err(err) => {
            // This should have been prevented by Narwhal batch verification.
            panic!(
                "Unexpected malformed transaction (failed to deserialize): {}\nBatchDigest={:?} Transaction={:?}",
                err, batch_digest, serialized_transaction
            );
        }
    }
}
