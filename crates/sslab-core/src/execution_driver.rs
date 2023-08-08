use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use crate::{types::ExecutableEthereumTransaction, executor::SerialExecutor, execution_storage::{MemoryStorage, ExecutionBackend}};
use mysten_metrics::{monitored_scope, spawn_monitored_task};
use parking_lot::RwLock;
use tokio::{
    sync::{mpsc::UnboundedReceiver, oneshot, Semaphore},
    time::sleep,
};
use tracing::{error, error_span, info, trace, Instrument};

// Execution should not encounter permanent failures, so any failure can and needs
// to be retried.
pub const EXECUTION_MAX_ATTEMPTS: u32 = 10;
const EXECUTION_FAILURE_RETRY_INTERVAL: Duration = Duration::from_secs(1);
 
/// When a notification that a new pending certificate is received we activate
/// processing the certificate in a loop.
pub async fn execution_process(
    execute_state: Weak<SerialExecutor>,
    execution_store: Arc<RwLock<MemoryStorage>>,
    mut rx_ready_certificates: UnboundedReceiver<ExecutableEthereumTransaction>,
    mut rx_execution_shutdown: oneshot::Receiver<()>,
) {
    info!("Starting pending certificates execution process.");

    // Rate limit for serial execution
    let limit = Arc::new(Semaphore::new(1));

    // Loop whenever there is a signal that a new certificates is ready to process.
    loop {
        let _scope = monitored_scope("ExecutionDriver::loop");

        let certificate: ExecutableEthereumTransaction;
        tokio::select! {
            result = rx_ready_certificates.recv() => {
                if let Some(cert) = result {
                    certificate = cert;
                } else {
                    // Should only happen after the AuthorityState has shut down and tx_ready_certificate
                    // has been dropped by TransactionManager.
                    info!("No more certificate will be received. Exiting executor ...");
                    return;
                };
            }
            _ = &mut rx_execution_shutdown => {
                info!("Shutdown signal received. Exiting executor ...");
                return;
            }
        };

        let executor = if let Some(executor) = execute_state.upgrade() {
            executor
        } else {
            // Terminate the execution if executor has already shutdown, even if there can be more
            // items in rx_ready_certificates.
            info!("Executor state has shutdown. Exiting ...");
            return;
        };
        // executor.metrics.execution_driver_dispatch_queue.dec();

        let digest = *certificate.digest();
        trace!(?digest, "Pending certificate execution activated.");

        let limit = limit.clone();
        // hold semaphore permit until task completes. unwrap ok because we never close
        // the semaphore in this context.
        let permit = limit.acquire_owned().await.unwrap();
        let execution_backend = execution_store.read();
        if let Ok(true) = execution_backend.is_tx_already_executed(&digest) {
            return;
        }

        // Certificate execution can take significant time, so run it in a separate task.
        spawn_monitored_task!(async move {
            let _scope = monitored_scope("ExecutionDriver::task");
            let _guard = permit;

            let mut attempts = 0;
            loop {
                attempts += 1;
                let res = executor
                    .try_execute_immediately(&certificate)
                    .await;
                if let Err(e) = res {
                    if attempts == EXECUTION_MAX_ATTEMPTS {
                        panic!("Failed to execute certified certificate {digest:?} after {attempts} attempts! error={e} certificate={certificate:?}");
                    }
                    // Assume only transient failure can happen. Permanent failure is probably
                    // a bug. There is nothing that can be done to recover from permanent failures.
                    error!(tx_digest=?digest, "Failed to execute certified certificate {digest:?}! attempt {attempts}, {e}");
                    sleep(EXECUTION_FAILURE_RETRY_INTERVAL).await;
                } else {
                    break;
                }
            }
            // executor
            //     .metrics
            //     .execution_driver_executed_certificates
            //     .inc();
        }.instrument(error_span!("execution_driver", tx_digest = ?digest)));
    }
}