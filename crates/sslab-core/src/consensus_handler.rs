use mysten_metrics::monitored_scope;
use async_trait::async_trait;
use fastcrypto::hash::Hash as _Hash;
use narwhal_executor::ExecutionState;
use narwhal_types::{BatchAPI, CertificateAPI, ConsensusOutput, HeaderAPI};
// use sui_core::authority::AuthorityMetrics;
use tracing::{info, instrument};
use crate::types::{ExecutableEthereumTransaction, EthereumTransaction};
use core::panic;
use std::sync::Arc;

pub struct SimpleConsensusHandler {
    tx_consensus_certificate: tokio::sync::mpsc::Sender<Vec<ExecutableEthereumTransaction>>,
    // metrics: Arc<AuthorityMetrics>,
}

impl SimpleConsensusHandler {
    pub fn new(
        tx_consensus_certificate: tokio::sync::mpsc::Sender<Vec<ExecutableEthereumTransaction>>,
        // metrics: Arc<AuthorityMetrics>,
    ) -> Self {
        Self {
            tx_consensus_certificate,
            // metrics
        }
    }
}

#[async_trait]
impl ExecutionState for SimpleConsensusHandler {

    /// This function will be called by Narwhal, after Narwhal sequenced this certificate.
    #[instrument(level = "trace", skip_all)]
    async fn handle_consensus_output(&self, consensus_output: ConsensusOutput) {
        let _scope = monitored_scope("HandleConsensusOutput");

        let round = consensus_output.sub_dag.leader_round();

        /* (serialized, transaction, output_cert) */
        let mut transactions : Vec<ExecutableEthereumTransaction> = vec![];
        let timestamp = consensus_output.sub_dag.commit_timestamp();
        // let leader_author = consensus_output.sub_dag.leader.header().author();

        info!(
            "Received consensus output {:?} at leader round {}, subdag index {}, timestamp {} ",
            consensus_output.digest(),
            round,
            consensus_output.sub_dag.sub_dag_index,
            timestamp.clone(),
        );

        // self.metrics
        //     .consensus_committed_subdags
        //     .with_label_values(&[&leader_author.to_string()])
        //     .inc();
        for (cert, batches) in consensus_output
            .sub_dag
            .certificates
            .iter()
            .zip(consensus_output.batches.iter())
        {
            assert_eq!(cert.header().payload().len(), batches.len());
            // let author = cert.header().author();
            // self.metrics
            //     .consensus_committed_certificates
            //     .with_label_values(&[&author.to_string()])
            //     .inc();
            let mut _batch_tx: Vec<EthereumTransaction> = vec![];
            let output_cert = Arc::new(cert.clone());
            for batch in batches {
                assert!(output_cert.header().payload().contains_key(&batch.digest()));
                // self.metrics.consensus_handler_processed_batches.inc();
                for serialized_transaction in batch.transactions() {

                    let transaction = match bcs::from_bytes::<EthereumTransaction>(
                        serialized_transaction,
                    ) {
                        Ok(transaction) => transaction,
                        Err(err) => {
                            // This should have been prevented by Narwhal batch verification.
                            panic!(
                                "Unexpected malformed transaction (failed to deserialize): {}\nCertificate={:?} BatchDigest={:?} Transaction={:?}",
                                err, output_cert, batch.digest(), serialized_transaction
                            );
                        }
                    };
                    // self.metrics
                    //     .consensus_handler_processed
                    //     .with_label_values(&[classify(&transaction)])
                    //     .inc();
                    _batch_tx.push(transaction);
                }
            }
            transactions.push(ExecutableEthereumTransaction::new(_batch_tx, output_cert));
        }

        // self.metrics
        // .consensus_handler_processed_bytes
        // .inc_by(bytes as u64);


        let _ = self.tx_consensus_certificate
            .send(transactions)
            .await;
    }

    async fn last_executed_sub_dag_index(&self) -> u64 {
        0
    }

}