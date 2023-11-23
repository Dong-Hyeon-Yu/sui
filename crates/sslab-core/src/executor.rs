use std::collections::BTreeMap;
use evm::{ExitReason, backend::{Apply, Log}, executor::stack::RwSet};
use mysten_metrics::spawn_monitored_task;
use sui_types::error::SuiError;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, warn, info};

use crate::{
    types::{ExecutableEthereumBatch, EthereumTransaction, ExecutableConsensusOutput, ExecutionResult}, 
    execution_storage::MemoryStorage
}; 

pub(crate) const DEFAULT_EVM_STACK_LIMIT:usize = 1024;
pub(crate) const DEFAULT_EVM_MEMORY_LIMIT:usize = usize::MAX; 

pub trait Executable {
    fn execute(&self, consensus_output: Vec<ExecutableEthereumBatch>);
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
                    // NOTE: This log entry is used to compute performance.
                    consensus_output.data().iter().for_each(|batch_digest|
                        info!("Received Batch -> {:?}", batch_digest.digest())
                    );
                }
            }
            self.execution_model.execute(consensus_output.data().to_owned());
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
    pub fn execute_tx(tx: &EthereumTransaction, snapshot: &MemoryStorage) -> Result<Option<(Vec<Apply>, Vec<Log>)>, SuiError> {
        let mut executor = snapshot.executor(tx.gas_limit(), false);

        let mut effect: Vec<Apply> = vec![];
        let mut log: Vec<Log> = vec![];
    
        if let Some(to_addr) = tx.to_addr() {
    
            let (reason, _) = & executor.transact_call(
                tx.caller(), *to_addr, tx.value(), tx.data().unwrap().to_owned().to_vec(), 
                tx.gas_limit(), tx.access_list()
            );
    
            match Self::process_transact_call_result(reason) {
                Ok(fail) => {
                    if fail {
                        return Ok(None);
                    } else {
                        // debug!("success to execute a transaction {}", tx.id());
                        (effect, log) = executor.into_state().deconstruct();
                        return Ok(Some((effect, log)));
                    }
                },
                Err(e) => return Err(e)
            }
        } else { 
            if let Some(data) = tx.data() {
                 // create EOA
                let init_code = data.to_vec();
                let (reason, _) = &executor.transact_create(tx.caller(), tx.value(), init_code.clone(), tx.gas_limit(), tx.access_list());
    
                match Self::process_transact_create_result(reason) {
                    Ok(fail) => {
                        if fail {
                            return Ok(None);
                        } else {
                            debug!("success to deploy a contract!");

                            (effect, log) = executor.into_state().deconstruct();
                            return Ok(Some((effect, log)));
                        }
                    },
                    Err(e) => return Err(e)
                    
                }
            } else {
                // create user account
                debug!("create user account: {:?} with balance {:?} and nonce {:?}", tx.caller(), tx.value(), tx.nonce());
                effect.push(Apply::Modify {
                    address: tx.caller(),
                    basic: evm::backend::Basic { balance: tx.value(), nonce: tx.nonce() },
                    code: None,
                    storage: BTreeMap::new(),
                    reset_storage: false,
                });
                log.push(Log {
                    address: tx.caller(),
                    topics: vec![],
                    data: vec![],
                });
                // Self::_process_local_effect(store, effect, log, &mut effects, &mut logs);
                return Ok(Some((effect, log)));
            }
        }
    }

    pub fn simulate_tx(tx: &EthereumTransaction, snapshot: &MemoryStorage) -> Result<Option<(Vec<Apply>, Vec<Log>, RwSet)>, SuiError> {
        let mut executor = snapshot.executor(tx.gas_limit(), true);

        let mut effect: Vec<Apply> = vec![];
        let mut log: Vec<Log> = vec![];

        if let Some(to_addr) = tx.to_addr() {
    
            let (reason, _) = & executor.transact_call(
                tx.caller(), *to_addr, tx.value(), tx.data().unwrap().to_owned().to_vec(), 
                tx.gas_limit(), tx.access_list()
            );
    
            match Self::process_transact_call_result(reason) {
                Ok(fail) => {
                    if fail {
                        return Ok(None);
                    } else {
                        // debug!("success to execute a transaction {}", tx.id());
                        let rw_set = executor.rw_set().unwrap().clone();
                        (effect, log) = executor.into_state().deconstruct();
                        return Ok(Some((effect, log, rw_set)));
                    }
                },
                Err(e) => return Err(e)
            }
        } else { 
            if let Some(data) = tx.data() {
                 // create EOA
                let init_code = data.to_vec();
                let (reason, _) = &executor.transact_create(tx.caller(), tx.value(), init_code.clone(), tx.gas_limit(), tx.access_list());
    
                match Self::process_transact_create_result(reason) {
                    Ok(fail) => {
                        if fail {
                            return Ok(None);
                        } else {
                            debug!("success to deploy a contract!");
                            let rw_set = executor.rw_set().unwrap().clone();
                            (effect, log) = executor.into_state().deconstruct();
                            return Ok(Some((effect, log, rw_set)));
                        }
                    },
                    Err(e) => return Err(e)
                    
                }
            } else {
                // create user account
                debug!("create user account: {:?} with balance {:?} and nonce {:?}", tx.caller(), tx.value(), tx.nonce());
                effect.push(Apply::Modify {
                    address: tx.caller(),
                    basic: evm::backend::Basic { balance: tx.value(), nonce: tx.nonce() },
                    code: None,
                    storage: BTreeMap::new(),
                    reset_storage: false,
                });
                log.push(Log {
                    address: tx.caller(),
                    topics: vec![],
                    data: vec![],
                });
                // Self::_process_local_effect(store, effect, log, &mut effects, &mut logs);
                return Ok(Some((effect, log, RwSet::new())));
            }
        }
    }
    
    fn process_transact_call_result(reason: &ExitReason) -> Result<bool, SuiError> {
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
    
    fn process_transact_create_result(reason: &ExitReason) -> Result<bool, SuiError> {
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

