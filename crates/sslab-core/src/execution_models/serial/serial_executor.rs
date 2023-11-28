use std::sync::Arc;

use tracing::{warn, info, trace};

use crate::{
    executor::{Executable, EvmExecutionUtils}, 
    types::{ExecutionResult, ExecutableEthereumBatch}, 
    execution_models::execution_storage::{SerialEVMStorage, backend::ExecutionBackend as _}
};


impl Executable for SerialExecutor {
    fn execute(&self, consensus_output: Vec<ExecutableEthereumBatch>) {

        for batch in consensus_output {
            let _ = self._execute(batch);
        }
    }
}


pub struct SerialExecutor {
    global_state: Arc<SerialEVMStorage>,
}

impl SerialExecutor {
    pub fn new(global_state: Arc<SerialEVMStorage>) -> Self {
        info!("Execution mode: 'serial'");
        Self {
            global_state
        }
    }

    pub fn _execute(&self, batch: ExecutableEthereumBatch) -> ExecutionResult {

        let state = self.global_state.snapshot();

        for tx in batch.data() {
            match EvmExecutionUtils::execute_tx(tx, &state.snapshot()) {
                Ok(Some((effect, _))) 
                    => state.apply_local_effect(effect),
                Ok(None) 
                    => trace!("{:?} may be reverted.", tx.id()),
                Err(e) 
                    => warn!("fail to execute a transaction {:?}", e)
            }
        }

        ExecutionResult::new(vec![batch.digest().clone()])
    }
}

