use std::{sync::Arc, time::Duration};

use crate::{
    types::ExecutableEthereumBatch, 
    execution_storage::{MemoryStorage, ExecutionBackend, ExecutionResult}, 
    executor::SerialExecutor};
use mysten_metrics::{monitored_scope, spawn_monitored_task};
use narwhal_types::{ConditionalBroadcastReceiver, BatchDigest};
use parking_lot::RwLock;
use tokio::{
    sync::mpsc::{UnboundedReceiver, Sender},
    time::sleep,
};
use tracing::{error, error_span, info, Instrument};

// Execution should not encounter permanent failures, so any failure can and needs
// to be retried.
pub const EXECUTION_MAX_ATTEMPTS: u32 = 10;
const EXECUTION_FAILURE_RETRY_INTERVAL: Duration = Duration::from_secs(1);
 
/// When a notification that a new pending certificate is received we activate
/// processing the certificate in a loop.
pub async fn execution_process(
    execute_state: Arc<RwLock<MemoryStorage>>,
    mut rx_ready_certificates: UnboundedReceiver<ExecutableEthereumBatch>,
    tx_commit_notification: Sender<(BatchDigest, ExecutionResult)>,
    mut rx_execution_shutdown: ConditionalBroadcastReceiver,
) {
    info!("Starting pending certificates execution process.");

    // Rate limit for serial execution
    // let limit = Arc::new(Semaphore::new(1));

    // Loop whenever there is a signal that a new certificates is ready to process.
    loop {
        let _scope = monitored_scope("ExecutionDriver::loop");

        let certificate: ExecutableEthereumBatch;
        tokio::select! {
            result = rx_ready_certificates.recv() => {
                if let Some(cert) = result {
                    certificate = cert;
                } else {
                    // Should only happen after the AuthorityState has shut down and tx_ready_certificate
                    // has been dropped by TransactionManager.
                    info!("No more batches will be received. Exiting executor ...");
                    return;
                };
            }
            _ = rx_execution_shutdown.receiver.recv() => {
                info!("Shutdown signal received. Exiting executor ...");
                return;
            }
        };

        let digest = *certificate.digest();

        // let limit = limit.clone();
        // hold semaphore permit until task completes. 
        // if let Some(permit) = limit.acquire_owned().await.ok() {

            // create copy of global state.
            let storage;
            {
                let _storage = execute_state.read();
                if let Ok(true) = _storage.is_tx_already_executed(&digest) {
                    continue;
                }
                storage = _storage.clone();
            }
    
            let tx_commit_notify = tx_commit_notification.clone();
            
            // Certificate execution can take significant time, so run it in a separate task.
            spawn_monitored_task!(async move {
                let _scope = monitored_scope("ExecutionDriver::task");
                // let _guard = permit;
    
                let mut execution = SerialExecutor::new(storage, tx_commit_notify);
    
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
            }.instrument(error_span!("execution_driver", tx_digest = ?digest)));
        // }
    }
}