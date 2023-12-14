use std::sync::Arc;

use sslab_execution::{
    executor::Executable, 
    evm_storage::{SerialEVMStorage, backend::ExecutionBackend}, 
    types::{ExecutableEthereumBatch, ExecutionResult}
};
use tracing::{warn, info, trace};



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

        let state = self.global_state.as_ref();

        for tx in batch.data() {
            match crate::evm_utils::execute_tx(tx, state) {
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

