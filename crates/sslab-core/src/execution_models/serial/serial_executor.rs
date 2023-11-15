use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc::Sender;
use tracing::{warn, info, trace};

use crate::{executor::{Executable, EvmExecutionUtils}, types::{ExecutionResult, ExecutableEthereumBatch}, execution_storage::MemoryStorage};


#[async_trait::async_trait]
impl Executable for SerialExecutor {
    async fn execute(&mut self, consensus_output: Vec<ExecutableEthereumBatch>, tx_execute_notification: &mut Sender<ExecutionResult>) {

        for batch in consensus_output {
            let result = self._execute(batch);
            let _ = tx_execute_notification.send(result).await;
        }
    }
}


pub struct SerialExecutor {
    global_state: Arc<RwLock<MemoryStorage>>,
}

impl SerialExecutor {
    pub fn new(global_state: Arc<RwLock<MemoryStorage>>) -> Self {
        info!("Execution mode: 'serial'");
        Self {
            global_state
        }
    }

    pub fn _execute(&mut self, batch: ExecutableEthereumBatch) -> ExecutionResult {

        let state = self.global_state.write();
        let snapshot = &mut state.snapshot();
        let effects = &mut vec![];
        let logs = &mut vec![];

        for tx in batch.data() {
            match EvmExecutionUtils::execute_tx(tx, snapshot, false) {
                Ok(Some((effect, log, _))) 
                    => EvmExecutionUtils::process_local_effect(snapshot, effect, log, effects, logs),
                Ok(None) 
                    => trace!("{:?} may be reverted.", tx.id()),
                Err(e) 
                    => warn!("fail to execute a transaction {:?}", e)
            }
        }

        ExecutionResult::new(vec![batch.digest().clone()])
    }
}

