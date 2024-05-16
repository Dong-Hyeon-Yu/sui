use std::sync::Arc;

use reth::{
    core::node_config::ConfigureEvmEnv,
    primitives::{
        revm::env::fill_tx_env, Address, BlockWithSenders, ChainSpec, Header, Receipt,
        TransactionSigned, U256,
    },
    providers::ProviderError,
    revm::{
        database::StateProviderDatabase,
        inspector_handle_register,
        interpreter::Host,
        primitives::{CfgEnvWithHandlerCfg, HandlerCfg, ResultAndState, SpecId},
        stack::{InspectorStack, InspectorStackConfig},
        Database, DatabaseCommit, Evm, EvmBuilder, Handler,
    },
};
use sslab_execution::{
    db::{SharableState, ThreadSafeCacheState},
    traits::Executable,
    BlockExecutionError, BlockValidationError, EthEvmConfig, ProviderFactoryMDBX,
};

use tokio::sync::Mutex;
use tracing::debug;

pub struct SerialExecutor {
    evm: Arc<
        Mutex<
            Evm<
                'static,
                InspectorStack,
                SharableState<Box<dyn Database<Error = ProviderError> + Send>>,
            >,
        >,
    >,
    chain_spec: Arc<ChainSpec>,
}

#[async_trait::async_trait(?Send)]
impl Executable for SerialExecutor {
    async fn execute(
        &mut self,
        consensus_output: BlockWithSenders,
    ) -> Result<(BlockWithSenders, Vec<Receipt>, u64), BlockExecutionError> {
        self._execute(consensus_output).await
    }

    fn new_with_db(
        db: ProviderFactoryMDBX,
        cached_state: Option<ThreadSafeCacheState>,
        chain_spec: Arc<ChainSpec>,
    ) -> Self {
        let db = SharableState::builder()
            .with_database_boxed(Box::new(StateProviderDatabase::new(db.latest().unwrap())))
            .with_cached_prestate(cached_state.unwrap_or_default())
            .with_bundle_update()
            .build();
        Self {
            evm: Arc::new(Mutex::new(
                EvmBuilder::default()
                    .with_db(db)
                    .with_external_context(InspectorStack::new(InspectorStackConfig::default()))
                    .with_handler_cfg(HandlerCfg::new(SpecId::ISTANBUL))
                    .build(),
            )),
            chain_spec,
        }
    }
}

impl SerialExecutor {
    // pub fn new(global_state: DBBox<'_, ProviderError>, chain_spec: Arc<ChainSpec>) -> Self {
    //     Self {
    //         evm: Arc::new(Mutex::new(
    //             EvmBuilder::default()
    //                 .with_db(global_state)
    //                 .with_external_context(InspectorStack::new(InspectorStackConfig::default()))
    //                 .with_handler_cfg(HandlerCfg::new(SpecId::ISTANBUL))
    //                 .build(),
    //         )),
    //         chain_spec,
    //     }
    // }

    /// Runs a single transaction in the configured environment and proceeds
    /// to return the result and state diff (without applying it).
    ///
    /// Assumes the rest of the block environment has been filled via `init_block_env`.
    async fn transact(
        &self,
        transaction: &TransactionSigned,
        sender: Address,
    ) -> Result<ResultAndState, BlockExecutionError> {
        let mut evm = self.evm.lock().await;

        // Fill revm structure.
        fill_tx_env(evm.tx_mut(), transaction, sender);

        let hash = transaction.hash();
        let should_inspect = evm.context.external.should_inspect(evm.env(), hash);
        let out = if should_inspect {
            // push inspector handle register.
            evm.handler
                .append_handler_register_plain(inspector_handle_register);
            let output = evm.transact();
            tracing::trace!(
                target: "evm",
                ?hash, ?output, ?transaction, env = ?evm.context.evm.env,
                "Executed transaction"
            );
            // pop last handle register
            evm.handler.pop_handle_register();
            output
        } else {
            // Main execution without needing the hash
            evm.transact()
        };

        out.map_err(move |e| {
            // Ensure hash is calculated for error log, if not already done
            BlockValidationError::EVM {
                hash: transaction.recalculate_hash(),
                error: e.into(),
            }
            .into()
        })
    }

    /// Initializes the config and block env.
    async fn init_env(&mut self, header: &Header) -> Result<(), BlockExecutionError> {
        let mut evm = self.evm.lock().await;

        let mut cfg: CfgEnvWithHandlerCfg =
            CfgEnvWithHandlerCfg::new_with_spec_id(evm.cfg().clone(), evm.spec_id());
        EthEvmConfig::fill_cfg_and_block_env(
            &mut cfg,
            evm.block_mut(),
            &self.chain_spec,
            header,
            U256::ZERO,
        );
        *evm.cfg_mut() = cfg.cfg_env;
        evm.handler = Handler::new(cfg.handler_cfg);

        // Applies the pre-block call to the EIP-4788 beacon block root contract.
        //
        // If cancun is not activated or the block is the genesis block, then this is a no-op, and no
        // state changes are made.
        reth::revm::state_change::apply_beacon_root_contract_call(
            &self.chain_spec,
            header.timestamp,
            header.number,
            header.parent_beacon_block_root,
            &mut evm,
        )?;

        Ok(())
    }

    #[inline]
    async fn _execute(
        &mut self,
        block: BlockWithSenders,
    ) -> Result<(BlockWithSenders, Vec<Receipt>, u64), BlockExecutionError> {
        self.init_env(&block.header).await?;
        let mut cumulative_gas_used = 0;
        let mut receipts = vec![];

        for (sender, tx) in block.transactions_with_sender() {
            let ResultAndState { result, state } = self.transact(tx, *sender).await?;

            cumulative_gas_used += result.gas_used();

            let receipt = match result {
                reth::revm::primitives::ExecutionResult::Success { logs, .. } => {
                    self.evm.lock().await.context.evm.db.commit(state);

                    Receipt {
                        tx_type: tx.tx_type(),
                        cumulative_gas_used,
                        logs: logs.into_iter().map(|log| log.into()).collect(),
                        success: true,
                    }
                }
                reth::revm::primitives::ExecutionResult::Revert { output, .. } => {
                    debug!("Transaction revert (by solidity revert): {:?}", output);
                    Receipt {
                        tx_type: tx.tx_type(),
                        cumulative_gas_used,
                        logs: vec![],
                        success: false,
                    }
                }
                reth::revm::primitives::ExecutionResult::Halt { reason, .. } => {
                    debug!("Transaction halt: {:?}", reason);
                    Receipt {
                        tx_type: tx.tx_type(),
                        cumulative_gas_used,
                        logs: vec![],
                        success: false,
                    }
                }
            };
            receipts.push(receipt);
        }

        Ok((block, receipts, cumulative_gas_used))
    }
}
