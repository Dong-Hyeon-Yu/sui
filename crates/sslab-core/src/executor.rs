use std::sync::Arc;
// use parking_lot::RwLock;
use tokio::sync::RwLock;
use evm::{ExitReason, backend::Backend};
use itertools::Itertools;
use tap::tap::TapFallible;
use mysten_metrics::{monitored_scope, metered_channel::Sender};
use sui_macros::fail_point_async;
use sui_types::{error::{SuiResult, ExecutionError}, digests::TransactionDigest};
use tokio::sync::{oneshot, Mutex, mpsc::error::SendError};
use tracing::{debug, info};
use crate::{types::ExecutableEthereumTransaction, execution_storage::{ExecutionBackend, MemoryStorage, ExecutionResult}}; 

pub(crate) const DEFAULT_EVM_STACK_LIMIT:usize = 1024;
pub(crate) const DEFAULT_EVM_MEMORY_LIMIT:usize = usize::MAX; 

pub struct SerialExecutor {

    execution_store: Arc<RwLock<MemoryStorage>>,
    notify_commit: Sender<(TransactionDigest, ExecutionResult)>,

    /// Shuts down the execution task. Used only in testing.
    #[allow(unused)]
    tx_execution_shutdown: Mutex<Option<oneshot::Sender<()>>>,
}

impl SerialExecutor {
    
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
        let database = self.execution_store.read().await;

        let mut effects = vec![];
        let mut logs = vec![];
        // TODO: apply..?
        for tx in certificate.data() {
            let mut executor = database.executor();
            let mut runtime = tx.execution_part(database.backend().code(*tx.to_addr()));
            
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
}
