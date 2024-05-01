use tokio::sync::mpsc::Receiver;
use tracing::debug;

use crate::types::{ExecutableConsensusOutput, ExecutableEthereumBatch};

#[async_trait::async_trait(?Send)]
pub trait Executable<T> {
    async fn execute(&self, consensus_output: Vec<ExecutableEthereumBatch<T>>);
}

pub struct ParallelExecutor<T: Clone, ExecutionModel: Executable<T> + Send + Sync> {
    rx_consensus_certificate: Receiver<ExecutableConsensusOutput<T>>,

    // rx_shutdown: ConditionalBroadcastReceiver,
    execution_model: ExecutionModel,
}

#[async_trait::async_trait(?Send)]
pub trait ExecutionComponent {
    async fn run(&mut self);
}

#[async_trait::async_trait(?Send)]
impl<T: Clone + Send + Sync, ExecutionModel: Executable<T> + Send + Sync> ExecutionComponent
    for ParallelExecutor<T, ExecutionModel>
{
    async fn run(&mut self) {
        while let Some(consensus_output) = self.rx_consensus_certificate.recv().await {
            debug!(
                "Received consensus output at leader round {}, subdag index {}, timestamp {} ",
                consensus_output.round(),
                consensus_output.sub_dag_index(),
                consensus_output.timestamp(),
            );
            cfg_if::cfg_if! {
                if #[cfg(feature = "benchmark")] {
                    use tracing::info;
                    // NOTE: This log entry is used to compute performance.
                    consensus_output.data().iter().for_each(|batch_digest|
                        info!("Received Batch -> {:?}", batch_digest.digest())
                    );
                }
            }
            self.execution_model
                .execute(consensus_output.data().to_owned())
                .await;
            cfg_if::cfg_if! {
                if #[cfg(feature = "benchmark")] {
                    // NOTE: This log entry is used to compute performance.
                    consensus_output.data().iter().for_each(|batch_digest|
                        info!("Executed Batch -> {:?}", batch_digest.digest())
                    );
                }
            }
        }
    }
}

impl<T: Clone, ExecutionModel: Executable<T> + Send + Sync> ParallelExecutor<T, ExecutionModel> {
    pub fn new(
        rx_consensus_certificate: Receiver<ExecutableConsensusOutput<T>>,
        // rx_shutdown: ConditionalBroadcastReceiver,
        execution_model: ExecutionModel,
    ) -> Self {
        Self {
            rx_consensus_certificate,
            // rx_shutdown,
            execution_model,
        }
    }
}
