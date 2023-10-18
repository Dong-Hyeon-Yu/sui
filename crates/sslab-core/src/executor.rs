
use std::collections::BTreeMap;
use ethers_core::types::H160;
use evm::{ExitReason, backend::{Apply, Log, MemoryBackend}, executor::stack::{StackExecutor, MemoryStackState, PrecompileFn}};
use itertools::Itertools;
use narwhal_types::BatchDigest;
use tap::tap::TapFallible;
use sui_macros::fail_point_async;
use sui_types::error::{SuiResult, ExecutionError, SuiError};
use tokio::sync::mpsc::{error::SendError, Sender, Receiver};
use tracing::{debug, info, warn, error, instrument, trace};

use crate::{
    types::{ExecutableEthereumBatch, EthereumTransaction}, 
    execution_storage::{ExecutionBackend, MemoryStorage, ExecutionResult}
}; 

pub(crate) const DEFAULT_EVM_STACK_LIMIT:usize = 1024;
pub(crate) const DEFAULT_EVM_MEMORY_LIMIT:usize = usize::MAX; 

pub struct SerialExecutor {

    execution_store: MemoryStorage,  // copy of global state.
    notify_commit: Sender<(BatchDigest, ExecutionResult)>,
}

impl SerialExecutor {

    pub fn new(
        execution_store: MemoryStorage,  // copy of global state.
        notify_commit: Sender<(BatchDigest, ExecutionResult)>,
    ) -> Self {
        Self {
            execution_store,
            notify_commit,
        }
    }
    
    pub async fn try_execute_immediately(
        &mut self,
        certificate: &ExecutableEthereumBatch,
        // execution_store: &Arc<MemoryStorage>,
    ) -> SuiResult<((), Option<ExecutionError>)> {  
        let tx_digest = *certificate.digest();
        debug!("try_execute_immediately");

        self.process_certificate(certificate)
            .await
            .tap_err(|e| info!(?tx_digest, "process_certificate failed: {e}"))
    }

    #[instrument(level="trace", skip_all)]
    async fn process_certificate(
        &mut self,
        certificate: &ExecutableEthereumBatch,
    ) -> SuiResult<((), Option<ExecutionError>)> {  
        debug!("process_certificate");

        let mut effects = vec![];
        let mut logs = vec![];
        let store = &mut self.execution_store;

        
        for tx in certificate.data() {
            
            let mut executor = store.executor(tx.gas_limit(), true);

            let (effect, log) = match _process_tx(tx, &mut executor) {
                Ok(Some((effect, log))) => {
                    (effect, log)
                },
                Ok(None) => {
                    continue;
                },
                Err(e) => {
                    return Err(e);
                }
            };

            _process_local_effect(store, effect, log, &mut effects, &mut logs);
        }

        fail_point_async!("crash");

        let _ = self.commit_cert_and_notify(
            certificate,
            ExecutionResult {
                effects,
                logs,
            },
        )
        .await;

        Ok(((), None))
    }

    async fn commit_cert_and_notify(
        &self,
        batch: &ExecutableEthereumBatch,
        execution_result : ExecutionResult,
        // _storage_guard: StorageGuard
    ) -> Result<(), SendError<(BatchDigest, ExecutionResult)>> {
        debug!("commit_cert_and_notify");
        // self.commit_certificate(certificate, execution_result, ).await;

        // Notifies transaction manager about transaction and output objects committed.
        // This provides necessary information to transaction manager to start executing
        // additional ready transactions.
        //
        // REQUIRED: this must be called after commit_certificate() (above), which writes output
        // objects into storage. Otherwise, the transaction manager may schedule a transaction
        // before the output objects are actually available.
         self.notify_commit.send((*batch.digest(), execution_result)).await
    }

    pub fn is_tx_already_executed(&self, tx_digest: &BatchDigest) -> SuiResult<bool> {
        self.execution_store.is_tx_already_executed(tx_digest)
    }
}

use std::sync::Arc;
use parking_lot::RwLock;
use tokio::task::JoinHandle;
use rayon::prelude::*;

use mysten_metrics::spawn_logged_monitored_task;
use crate::types::SimulatedTransaction;

pub struct ParallelSimulator { 
    global_state: Arc<RwLock<MemoryStorage>>,  // copy of global state.

    rx_consensus_certificate: Receiver<Vec<ExecutableEthereumBatch>>, 

    tx_simulation_result: Sender<Vec<SimulatedTransaction>>,  
}

impl ParallelSimulator {
    fn new(
        global_state: Arc<RwLock<MemoryStorage>>,  // copy of global state.
        rx_consensus_certificate: Receiver<Vec<ExecutableEthereumBatch>>, 
        tx_simulation_result: Sender<Vec<SimulatedTransaction>>,  
    ) -> Self {
        Self {
            global_state,
            rx_consensus_certificate,
            tx_simulation_result,
        }
    }

    fn simulate(&self, consensus_output: Vec<ExecutableEthereumBatch>) -> Vec<SimulatedTransaction> {

        let snapshot = &self.global_state.read().snapshot();

        let tx_list = consensus_output
                .iter()
                .flat_map(|batch| batch.data())
                .collect_vec();

        // Parallel simulation requires heavy cpu usages. 
        // CPU-bound jobs would make the I/O-bound tokio threads starve.
        // To this end, a separated thread pool need to be used for cpu-bound jobs.
        // a new thread is created, and a new thread pool is created on the thread. (specifically, rayon's thread pool is created)
        let (tx, rx) = std::sync::mpsc::channel::<Vec<SimulatedTransaction>>();

        let thread = std::thread::spawn(move || {
            let results: Vec<SimulatedTransaction> = tx_list
                .par_iter()
                .filter_map(|tx| {
                    let mut executor = snapshot.executor(tx.gas_limit(), true);
                    match _process_tx(tx, &mut executor) {
                        Ok(Some((effect, log))) => {
                            let rw_set = executor.rw_set().expect("rw_set must be exist in simulation mode.");
                            Some(SimulatedTransaction::new(tx.id(), rw_set.to_owned()))
                        },
                        _ => {
                            warn!("fail to execute a transaction {}", tx.id());
                            None
                        },
                    }
                })
                .collect();

            let mut count_try = 0;
            while tx.send(results).is_err() && count_try < 10{
                warn!("fail to send simulation result to the main thread. (retry: #{count_try}");
            }

            error!("fail to send simulation result to the main thread.");
        });

        match rx.recv() {
            Ok(result) => {
                result
            },
            Err(e) => {
                error!("fail to receive simulation result from the worker thread. {:?}", e);
                vec![]
            }
        }
    }

    async fn run(&mut self) {
        loop {

            tokio::select! {
                Some(consensus_output) = self.rx_consensus_certificate.recv() => {
                    debug!("Received a batch from consensus");
                    
                    let simulation_result = self.simulate(consensus_output);
                    let _ = self.tx_simulation_result.send(simulation_result);
                }
            }
        }
    }

    pub fn spawn(
        global_state: Arc<RwLock<MemoryStorage>>,  // copy of global state.
        rx_consensus_certificate: Receiver<Vec<ExecutableEthereumBatch>>, 
        tx_simulation_result: Sender<Vec<SimulatedTransaction>>,  
    ) -> JoinHandle<()> {
        spawn_logged_monitored_task!(
            Self::new(global_state, rx_consensus_certificate, tx_simulation_result).run(),
            "parallel_simulator::run()"
        )
    }
}

fn _process_tx(tx: &EthereumTransaction, executor: &mut StackExecutor<MemoryStackState<MemoryBackend>, BTreeMap<H160, PrecompileFn>>) -> Result<Option<(Vec<Apply>, Vec<Log>)>, SuiError> {

    let mut effect: Vec<Apply> = vec![];
    let mut log: Vec<Log> = vec![];

    if let Some(to_addr) = tx.to_addr() {

        let (reason, _) = executor.transact_call(
            tx.caller(), *to_addr, tx.value(), tx.data().unwrap().to_owned().to_vec(), 
            tx.gas_limit(), tx.access_list()
        );

        match _process_transact_call_result(reason) {
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
            let (e, _) = executor.transact_create(tx.caller(), tx.value(), init_code.clone(), tx.gas_limit(), tx.access_list());

            match _process_transact_create_result(e) {
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

fn _process_transact_call_result(reason: ExitReason) -> Result<bool, SuiError> {
    match reason {
        ExitReason::Succeed(_) => {
            Ok(false)
        }
        ExitReason::Revert(e) => {
            // do nothing: explicit revert is not an error
            trace!("tx execution revert: {:?}", e);
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

fn _process_transact_create_result(reason: ExitReason) -> Result<bool, SuiError> {
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

fn _process_local_effect(store: &mut MemoryStorage, effect: Vec<Apply>, log: Vec<Log>, effects: &mut Vec<Apply>, logs: &mut Vec<Log>) {
    trace!("process_local_effect: {effect:?}");
    effects.extend(effect.clone());
    logs.extend(log.clone());
    store.apply_local_effect(effect, log);
}