use std::sync::Arc;

use reth::{
    core::node_config::ConfigureEvmEnv,
    primitives::TransactionSignedEcRecovered,
    revm::{
        primitives::{CfgEnv, CfgEnvWithHandlerCfg, ResultAndState, SpecId},
        DatabaseCommit as _, Evm, EvmBuilder, InMemoryDB,
    },
};
use sslab_execution::{
    executor::Executable,
    types::{ExecutableEthereumBatch, ExecutionResult},
    EthEvmConfig,
};

use tokio::sync::Mutex;
use tracing::{debug, warn};

#[async_trait::async_trait(?Send)]
impl Executable<TransactionSignedEcRecovered> for SerialExecutor {
    async fn execute(
        &self,
        consensus_output: Vec<ExecutableEthereumBatch<TransactionSignedEcRecovered>>,
    ) {
        for batch in consensus_output {
            let _ = self._execute(batch).await;
        }
    }
}

pub struct SerialExecutor {
    evm: Arc<Mutex<Evm<'static, (), InMemoryDB>>>,
}

impl SerialExecutor {
    pub fn new(global_state: InMemoryDB) -> Self {
        let mut cfg_env = CfgEnv::default();
        cfg_env.chain_id = 9;
        let cfg_env = CfgEnvWithHandlerCfg::new_with_spec_id(cfg_env, SpecId::ISTANBUL);

        Self {
            evm: Arc::new(Mutex::new(
                EvmBuilder::default()
                    .with_db(global_state)
                    .with_cfg_env_with_handler_cfg(cfg_env)
                    .build(),
            )),
        }
    }

    #[inline]
    async fn _execute(
        &self,
        batch: ExecutableEthereumBatch<TransactionSignedEcRecovered>,
    ) -> ExecutionResult {
        let digest = batch.digest().clone();

        let mut evm = self.evm.lock().await;
        for tx in batch.take_data() {
            EthEvmConfig::fill_tx_env(evm.tx_mut(), tx.as_ref(), tx.signer(), ());

            match evm.transact() {
                Ok(ResultAndState { result, state }) => {
                    evm.context.evm.db.commit(state);
                    debug!(
                        "Transaction failed (but this is normal evm error): {:?}",
                        result
                    );
                }
                Err(e) => warn!("fail to execute a transaction {:?}", e),
            }
        }

        ExecutionResult::new(vec![digest])
    }
}
