use std::sync::Arc;

use sslab_execution::{
    executor::Executable, 
    evm_storage::{SerialEVMStorage, backend::ExecutionBackend}, 
    types::{ExecutableEthereumBatch, ExecutionResult}
};
use tracing::{warn, info, trace};


#[async_trait::async_trait]
impl Executable for SerialExecutor {
    async fn execute(&self, consensus_output: Vec<ExecutableEthereumBatch>) {

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

        let state = self.global_state.clone();

        let digest = batch.digest().clone();

        std::thread::spawn(move || {
            for tx in batch.data() {
                match crate::evm_utils::execute_tx(tx, state.as_ref()) {
                    Ok(Some((effect, _))) 
                        => state.apply_local_effect(effect),
                    Ok(None) 
                        => trace!("{:?} may be reverted.", tx.digest_u64()),
                    Err(e) 
                        => warn!("fail to execute a transaction {:?}", e)
                }
            }
        }).join().expect("fail to execute transactions");
        
        ExecutionResult::new(vec![digest])
    }
}

