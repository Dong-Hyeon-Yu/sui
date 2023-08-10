use std::{sync::Arc, time::Duration};

use crate::{types::ExecutableEthereumTransaction, executor::SerialExecutor};
use mysten_metrics::{monitored_scope, spawn_monitored_task};
use narwhal_types::ConditionalBroadcastReceiver;
use tokio::{
    sync::{mpsc::UnboundedReceiver, Semaphore},
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
    execute_state: Arc<SerialExecutor>,
    mut rx_ready_certificates: UnboundedReceiver<ExecutableEthereumTransaction>,
    mut rx_execution_shutdown: ConditionalBroadcastReceiver,
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
            _ = rx_execution_shutdown.receiver.recv() => {
                info!("Shutdown signal received. Exiting executor ...");
                return;
            }
        };

        // executor.metrics.execution_driver_dispatch_queue.dec();

        let digest = *certificate.digest();
        trace!(?digest, "Pending certificate execution activated.");

        let limit = limit.clone();
        // hold semaphore permit until task completes. unwrap ok because we never close
        // the semaphore in this context.
        let permit = limit.acquire_owned().await.unwrap();
        if let Ok(true) = execute_state.is_tx_already_executed(&digest) {
            return;
        }

        let execution = execute_state.clone();

        // Certificate execution can take significant time, so run it in a separate task.
        spawn_monitored_task!(async move {
            let _scope = monitored_scope("ExecutionDriver::task");
            let _guard = permit;

            let mut attempts = 0;
            loop {
                attempts += 1;
                let res = execution
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