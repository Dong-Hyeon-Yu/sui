
use std::collections::BTreeMap;

use evm::{ExitReason, backend::{Apply, Log}};
use narwhal_types::BatchDigest;
use tap::tap::TapFallible;
use sui_macros::fail_point_async;
use sui_types::error::{SuiResult, ExecutionError, SuiError};
use tokio::sync::mpsc::{error::SendError, Sender};
use tracing::{debug, info, warn, instrument, trace};
use crate::{types::ExecutableEthereumBatch, execution_storage::{ExecutionBackend, MemoryStorage, ExecutionResult}}; 

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

            let mut effect: Vec<Apply> = vec![];
            let mut log: Vec<Log> = vec![];
            
            let mut executor = store.executor(tx.gas_limit());

            if let Some(to_addr) = tx.to_addr() {

                let (reason, _) = executor.transact_call(
                    tx.caller(), *to_addr, tx.value(), tx.data().unwrap().to_owned().to_vec(), 
                    tx.gas_limit(), tx.access_list()
                );

                match Self::_process_transact_call_result(reason) {
                    Ok(fail) => {
                        if fail {
                            continue;
                        } else {
                            // debug!("success to execute a transaction {}", tx.id());
                            (effect, log) = executor.into_state().deconstruct();
                            Self::_process_local_effect(store, effect, log, &mut effects, &mut logs);
                        }
                    },
                    Err(e) => return Err(e)
                }
            } else { 
                if let Some(data) = tx.data() {
                     // create EOA
                    let init_code = data.to_vec();
                    let (e, _) = executor.transact_create(tx.caller(), tx.value(), init_code.clone(), tx.gas_limit(), tx.access_list());

                    match Self::_process_transact_create_result(e) {
                        Ok(fail) => {
                            if fail {
                                continue;
                            } else {
                                debug!("success to deploy a contract!");
                                (effect, log) = executor.into_state().deconstruct();
                                Self::_process_local_effect(store, effect, log, &mut effects, &mut logs);
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
                    Self::_process_local_effect(store, effect, log, &mut effects, &mut logs);
                }
            }
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
