
use std::{sync::Arc, collections::BTreeMap};

use ethers_core::types::{U256, H160};
use evm::{ExitReason, backend::{Apply, Log}};
use itertools::Itertools;
use parking_lot::RwLock;
use tap::tap::TapFallible;
use mysten_metrics::monitored_scope;
use sui_macros::fail_point_async;
use sui_types::{error::{SuiResult, ExecutionError}, digests::TransactionDigest};
use tokio::sync::mpsc::{error::SendError, Sender};
use tracing::{debug, info, warn};
use crate::{types::ExecutableEthereumTransaction, execution_storage::{ExecutionBackend, MemoryStorage, ExecutionResult}}; 

pub(crate) const DEFAULT_EVM_STACK_LIMIT:usize = 1024;
pub(crate) const DEFAULT_EVM_MEMORY_LIMIT:usize = usize::MAX; 

pub struct SerialExecutor {

    execution_store: Arc<RwLock<MemoryStorage>>,
    notify_commit: Sender<(TransactionDigest, ExecutionResult)>,
}

impl SerialExecutor {

    pub fn new(
        execution_store: Arc<RwLock<MemoryStorage>>,
        notify_commit: Sender<(TransactionDigest, ExecutionResult)>,
    ) -> Self {
        Self {
            execution_store,
            notify_commit,
        }
    }
    
    // #[instrument(level = "trace", skip_all)]
    pub async fn try_execute_immediately(
        &self,
        certificate: &ExecutableEthereumTransaction,
        // execution_store: &Arc<MemoryStorage>,
    ) -> SuiResult<((), Option<ExecutionError>)> {  //TODO: return type
        let _scope = monitored_scope("Execution::try_execute_immediately");
        let tx_digest = *certificate.digest();
        debug!("execute_certificate_internal");

        // let database = self.execution_store.read();

        self.process_certificate(certificate)
            .await
            .tap_err(|e| info!(?tx_digest, "process_certificate failed: {e}"))
    }

    // #[instrument(level = "trace", skip_all)]
    pub async fn process_certificate(
        &self,
        certificate: &ExecutableEthereumTransaction,
    ) -> SuiResult<((), Option<ExecutionError>)> {  

        let mut effects = vec![];
        let mut logs = vec![];
        
        {
            let store = self.execution_store.read();
            for tx in certificate.data() {
                
                let mut executor = store.executor();

                match tx.to_addr() {
                    Some(to_addr) => {
                        let mut runtime = tx.execution_part(store.code(*to_addr));
                
                        match executor.execute(&mut runtime) {
                            ExitReason::Succeed(_) => {
                                let (effect, log) = executor.into_state().deconstruct();
            
                                effects.extend(effect.into_iter().collect_vec());
                                logs.extend(log.into_iter().collect_vec());
                            }
                            ExitReason::Revert(_) => {
                                // do nothing: explicit revert is not an error
                                continue;
                            }
                            ExitReason::Error(_) => {
                                // do nothing: normal EVM error
                                continue;
                            }
                            ExitReason::Fatal(_) => {
                                return Err(sui_types::error::SuiError::ExecutionError(String::from("Fatal error occurred on EVM!")));
                            }
                        }
                    }
                    None => {  // create address
                        match tx.data() {  
                            Some(data) => { // create contract
                                let init_code = data.to_owned().to_vec();
                                let (_, addr) = executor.transact_create(tx.caller(), tx.value(), init_code.clone(), tx.gas_limit(), tx.access_list());
                            
                                match addr.try_into() as Result<[u8; 20], _> {
                                    Ok(addr) => {
                                        info!("create contract address: {:?}", addr);
                                        effects.push(Apply::Modify {
                                            address: H160::from(addr),
                                            basic: evm::backend::Basic { balance: U256::zero(), nonce: U256::zero() },
                                            code: Some(init_code),
                                            storage: BTreeMap::new(),
                                            reset_storage: false,
                                        });
                                        logs.push(Log {
                                            address: H160::from(addr),
                                            topics: vec![],
                                            data: vec![],
                                        });
                                    },
                                    Err(e) => warn!("failed to create contract: {:?}", e)
                                };

                                
                            },
                            None => {  // create user account
                                info!("create user account: {:?}", tx.caller());
                                effects.push(Apply::Modify {
                                    address: tx.caller(),
                                    basic: evm::backend::Basic { balance: tx.value(), nonce: tx.nonce() },
                                    code: None,
                                    storage: BTreeMap::new(),
                                    reset_storage: false,
                                });
                                logs.push(Log {
                                    address: tx.caller(),
                                    topics: vec![],
                                    data: vec![],
                                });
                            }
                        }
                    }
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

    async fn commit_cert_and_notify(
        &self,
        certificate: &ExecutableEthereumTransaction,
        execution_result : ExecutionResult,
        // _storage_guard: StorageGuard
    ) -> Result<(), SendError<(TransactionDigest, ExecutionResult)>> {
        let _scope: Option<mysten_metrics::MonitoredScopeGuard> =
            monitored_scope("Execution::commit_cert_and_notify");

        // self.commit_certificate(certificate, execution_result, ).await;

        // Notifies transaction manager about transaction and output objects committed.
        // This provides necessary information to transaction manager to start executing
        // additional ready transactions.
        //
        // REQUIRED: this must be called after commit_certificate() (above), which writes output
        // objects into storage. Otherwise, the transaction manager may schedule a transaction
        // before the output objects are actually available.
         self.notify_commit.send((*certificate.digest(), execution_result)).await

        // Update metrics.
        // self.metrics.total_effects.inc();
        // self.metrics.total_certs.inc();

        // self.metrics
        //     .num_input_objs
        //     .observe(input_object_count as f64);
        // self.metrics
        //     .num_shared_objects
        //     .observe(shared_object_count as f64);
        // self.metrics.batch_size.observe(
        //     certificate
        //         .data()
        //         .intent_message()
        //         .value
        //         .kind()
        //         .num_commands() as f64,
        // );
    }

    pub fn is_tx_already_executed(&self, tx_digest: &TransactionDigest) -> SuiResult<bool> {
        self.execution_store.read().is_tx_already_executed(tx_digest)
    }
}
