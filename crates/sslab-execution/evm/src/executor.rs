use evm::ExitReason;
use sui_types::error::SuiError;
use tokio::sync::mpsc::Receiver;
use tracing::{debug, warn};

use crate::types::{ExecutableEthereumBatch, ExecutableConsensusOutput}; 

#[async_trait::async_trait]
pub trait Executable {
    async fn execute(&self, consensus_output: Vec<ExecutableEthereumBatch>);
}


pub struct ParallelExecutor<ExecutionModel: Executable + Send + Sync> { 

    rx_consensus_certificate: Receiver<ExecutableConsensusOutput>, 

    // rx_shutdown: ConditionalBroadcastReceiver,

    execution_model: ExecutionModel
}

#[async_trait::async_trait]
pub trait ExecutionComponent {
    async fn run(&mut self);
}

#[async_trait::async_trait]
impl<ExecutionModel: Executable + Send + Sync> ExecutionComponent for ParallelExecutor<ExecutionModel> {
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
            self.execution_model.execute(consensus_output.data().to_owned()).await;
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

impl<ExecutionModel: Executable + Send + Sync> ParallelExecutor<ExecutionModel> {
    pub fn new(
        rx_consensus_certificate: Receiver<ExecutableConsensusOutput>, 
        // rx_shutdown: ConditionalBroadcastReceiver,
        execution_model: ExecutionModel
    ) -> Self {
        Self {
            rx_consensus_certificate,
            // rx_shutdown,
            execution_model
        }
    }
}


pub struct EvmExecutionUtils;

impl EvmExecutionUtils {
    
    pub fn process_transact_call_result(reason: &ExitReason) -> Result<bool, SuiError> {
        match reason {
            ExitReason::Succeed(_) => {
                Ok(false)
            }
            ExitReason::Revert(e) => {
                // do nothing: explicit revert is not an error
                debug!("tx execution revert: {:?}", e);
                Ok(true)
            }
            ExitReason::Error(e) => {
                // do nothing: normal EVM error
                warn!("tx execution error: {:?}", e);
                Ok(true)
            }
            ExitReason::Fatal(e) => {
                warn!("tx execution fatal error: {:?}", e);
                Err(SuiError::ExecutionError(String::from("Fatal error occurred on EVM!")))
            }
        }
    }
    
    pub fn process_transact_create_result(reason: &ExitReason) -> Result<bool, SuiError> {
        match reason {
            ExitReason::Succeed(_) => {
                Ok(false)
            }
            ExitReason::Revert(e) => {
                // do nothing: explicit revert is not an error
                debug!("fail to deploy contract: {:?}", e);
                Ok(true)
            }
            ExitReason::Error(e) => {
                // do nothing: normal EVM error
                warn!("fail to deploy contract: {:?}", e);
                Ok(true)
            }
            ExitReason::Fatal(e) => {
                warn!("fatal error occurred when deploying contract: {:?}", e);
                Err(SuiError::ExecutionError(String::from("Fatal error occurred on EVM!")))
            }
        }
    }
}

