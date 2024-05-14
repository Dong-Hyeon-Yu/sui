pub mod db;
pub mod evm_processor;
pub mod evm_storage;
pub mod executor;
pub mod traits;
pub mod transaction_validator;
pub mod types;
pub mod utils;

pub use reth_interfaces::executor::BlockExecutionError;
pub use reth_node_ethereum::EthEvmConfig;

pub mod revm_utiles {

    use rayon::prelude::*;
    use std::sync::Arc;

    use crate::{
        db::SharableState,
        types::{ExecutableEthereumBatch, NOT_SUPPORT},
    };
    use narwhal_types::BatchDigest;
    use reth::{
        core::node_config::ConfigureEvm,
        primitives::{
            revm::env::fill_tx_env, Address, BlockNumber, ChainSpec, Hardfork, Header, Receipts,
            TransactionSigned,
        },
        providers::{BundleStateWithReceipts, ProviderError},
        revm::{
            database::StateProviderDatabase,
            inspector_handle_register,
            interpreter::Host,
            primitives::{CfgEnvWithHandlerCfg, ResultAndState},
            stack::{InspectorStack, InspectorStackConfig},
            Evm, Handler, StateDBBox,
        },
    };
    use reth_interfaces::executor::{BlockExecutionError, BlockValidationError};

    pub type EvmWithSharableState<'a, DB> =
        Evm<'a, InspectorStack, SharableState<StateProviderDatabase<DB>>>;

    /// Initializes the config and block env.
    pub fn init_evm<'a, EvmConfig: ConfigureEvm>(
        header: &Header,
        chain_spec: &Arc<ChainSpec>,
        evm_config: &EvmConfig,
        db: StateDBBox<'a, ProviderError>,
    ) -> Result<Evm<'a, InspectorStack, StateDBBox<'a, ProviderError>>, BlockExecutionError> {
        let stack = InspectorStack::new(InspectorStackConfig::default());
        let mut evm = evm_config.evm_with_inspector(db, stack);

        // Set state clear flag.
        let state_clear_flag = chain_spec
            .fork(Hardfork::SpuriousDragon)
            .active_at_block(header.number);
        evm.context.evm.db.set_state_clear_flag(state_clear_flag);

        let mut cfg: CfgEnvWithHandlerCfg =
            CfgEnvWithHandlerCfg::new_with_spec_id(evm.cfg().clone(), evm.spec_id());
        EvmConfig::fill_cfg_and_block_env(
            &mut cfg,
            evm.block_mut(),
            chain_spec,
            header,
            NOT_SUPPORT,
        );
        *evm.cfg_mut() = cfg.cfg_env;
        evm.handler = Handler::new(cfg.handler_cfg);

        // Applies the pre-block call to the EIP-4788 beacon block root contract.
        //
        // If cancun is not activated or the block is the genesis block, then this is a no-op, and no
        // state changes are made.
        reth::revm::state_change::apply_beacon_root_contract_call(
            &chain_spec,
            header.timestamp,
            header.number,
            header.parent_beacon_block_root,
            &mut evm,
        )?;

        Ok(evm)
    }

    pub fn take_output_state(
        evm: &mut Evm<'_, InspectorStack, StateDBBox<'_, ProviderError>>,
        first_block: Option<BlockNumber>,
        receipts: Receipts,
    ) -> BundleStateWithReceipts {
        // self.stats.log_debug(); //TODO: move this code outside of this function.
        BundleStateWithReceipts::new(
            evm.context.evm.db.take_bundle(),
            receipts,
            first_block.unwrap_or_default(),
        )
    }

    pub fn size_hint<'a>(
        evm: &Evm<'a, InspectorStack, StateDBBox<'a, ProviderError>>,
    ) -> Option<usize> {
        Some(evm.context.evm.db.bundle_size_hint())
    }

    /// Runs a single transaction in the configured environment and proceeds
    /// to return the result and state diff (without applying it).
    ///
    /// Assumes the rest of the block environment has been filled via `init_block_env`.
    pub fn transact(
        evm: &mut Evm<'_, InspectorStack, StateDBBox<'_, ProviderError>>,
        transaction: &TransactionSigned,
        sender: Address,
    ) -> Result<ResultAndState, BlockExecutionError> {
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

    #[inline]
    pub async fn _unpack_batches(
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> (Vec<BatchDigest>, Vec<TransactionSigned>) {
        let (send, recv) = tokio::sync::oneshot::channel();

        rayon::spawn(move || {
            let (digests, batches): (Vec<_>, Vec<_>) = consensus_output
                .into_par_iter()
                .map(|batch| {
                    let ExecutableEthereumBatch { digest, data } = batch;
                    (digest, data)
                })
                .unzip();
            let tx_list = batches.into_iter().flatten().collect::<Vec<_>>();

            let _ = send.send((digests, tx_list)).unwrap();
        });

        recv.await.unwrap()
    }
}
