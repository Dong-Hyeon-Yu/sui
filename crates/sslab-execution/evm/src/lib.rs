pub mod db;
pub mod evm_processor;
pub mod executor;
pub mod traits;
pub mod transaction_validator;
pub mod types;

#[cfg(any(feature = "benchmark", test))]
pub mod utils;

use std::{path::Path, sync::Arc};

use reth::{
    core::init::init_genesis,
    primitives::ChainSpec,
    providers::{providers::BlockchainProvider, ProviderFactory},
};
use reth_db::{init_db, DatabaseEnv};

pub type ProviderFactoryMDBX = ProviderFactory<DatabaseEnv>;
pub type BlockchainProviderMDBX<Tree> = BlockchainProvider<DatabaseEnv, Tree>;

pub fn get_provider_factory(chain_spec: Arc<ChainSpec>) -> ProviderFactoryMDBX {
    use reth_db::open_db_read_only;
    let path = std::env::var("RETH_DB_PATH").unwrap_or_else(|_| "./.db/reth/test".to_string());

    ProviderFactoryMDBX::new(
        open_db_read_only(Path::new(path.as_str()), Default::default()).unwrap(),
        chain_spec,
    )
}

pub fn get_provider_factory_rw(chain_spec: Arc<ChainSpec>) -> ProviderFactoryMDBX {
    let path = std::env::var("RETH_DB_PATH").unwrap_or_else(|_| "./.db/reth/test".to_string());
    let db = init_db(path, Default::default()).unwrap();
    let _ = init_genesis(db.clone(), chain_spec.clone());
    ProviderFactoryMDBX::new(db, chain_spec)
}

pub use reth_interfaces::executor::{BlockExecutionError, BlockValidationError};
pub use reth_node_ethereum::EthEvmConfig;

pub mod revm_utiles {

    use rayon::prelude::*;
    use reth_node_ethereum::EthEvmConfig;
    use std::sync::Arc;

    use crate::{
        db::{SharableState, SharableStateDBBox},
        types::{ExecutableEthereumBatch, NOT_SUPPORT},
    };
    use narwhal_types::BatchDigest;
    use reth::{
        core::node_config::{ConfigureEvm, ConfigureEvmEnv as _},
        primitives::{
            revm::env::fill_tx_env, Address, ChainSpec, Hardfork, Header, TransactionSigned,
        },
        providers::ProviderError,
        revm::{
            database::StateProviderDatabase,
            inspector_handle_register,
            interpreter::Host,
            primitives::{CfgEnvWithHandlerCfg, ResultAndState},
            stack::{InspectorStack, InspectorStackConfig},
            Evm, Handler,
        },
    };
    use reth_interfaces::executor::{BlockExecutionError, BlockValidationError};

    pub type EvmWithSharableState<'a, DB> =
        Evm<'a, InspectorStack, SharableState<StateProviderDatabase<DB>>>;

    /// Initializes the config and block env.
    pub fn init_evm<'a>(
        header: &Header,
        chain_spec: &Arc<ChainSpec>,
        db: SharableStateDBBox<'a, ProviderError>,
    ) -> Result<Evm<'a, InspectorStack, SharableStateDBBox<'a, ProviderError>>, BlockExecutionError>
    {
        let stack = InspectorStack::new(InspectorStackConfig::default());
        let mut evm = EthEvmConfig::default().evm_with_inspector(db, stack);

        // Set state clear flag.
        let state_clear_flag = chain_spec
            .fork(Hardfork::SpuriousDragon)
            .active_at_block(header.number);
        evm.context.evm.db.set_state_clear_flag(state_clear_flag);

        let mut cfg: CfgEnvWithHandlerCfg =
            CfgEnvWithHandlerCfg::new_with_spec_id(evm.cfg().clone(), evm.spec_id());
        EthEvmConfig::fill_cfg_and_block_env(
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

    pub fn size_hint<'a>(
        evm: &Evm<'a, InspectorStack, SharableStateDBBox<'a, ProviderError>>,
    ) -> Option<usize> {
        Some(evm.context.evm.db.bundle_size_hint())
    }

    /// Runs a single transaction in the configured environment and proceeds
    /// to return the result and state diff (without applying it).
    ///
    /// Assumes the rest of the block environment has been filled via `init_block_env`.
    pub fn transact(
        evm: &mut Evm<'_, InspectorStack, SharableStateDBBox<'_, ProviderError>>,
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
    pub async fn unpack_batches(
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
