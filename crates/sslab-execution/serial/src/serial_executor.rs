use sslab_execution::{
    evm_storage::{backend::ExecutionBackend as _, SerialEVMStorage},
    executor::Executable,
    types::{ExecutableEthereumBatch, ExecutionResult},
};
use tracing::{info, trace, warn};

#[async_trait::async_trait(?Send)]
impl Executable for SerialExecutor {
    async fn execute(&mut self, consensus_output: Vec<ExecutableEthereumBatch>) {
        for batch in consensus_output {
            let _ = self._execute(batch);
        }
    }
}

pub struct SerialExecutor {
    global_state: SerialEVMStorage,
}

impl SerialExecutor {
    pub fn new(global_state: SerialEVMStorage) -> Self {
        info!("Execution mode: 'serial'");
        Self { global_state }
    }

    pub fn _execute(&mut self, batch: ExecutableEthereumBatch) -> ExecutionResult {
        let digest = batch.digest().clone();

        for tx in batch.data() {
            match crate::evm_utils::execute_tx(tx, self.global_state.as_ref()) {
                Ok(Some((effect, _))) => self.global_state.apply_local_effect(effect),
                Ok(None) => trace!("{:?} may be reverted.", tx.digest_u64()),
                Err(e) => warn!("fail to execute a transaction {:?}", e),
            }
        }

        ExecutionResult::new(vec![digest])
    }
}
