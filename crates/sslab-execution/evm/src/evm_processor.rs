use async_trait::async_trait;
use reth::{
    primitives::{
        Block, BlockNumber, BlockWithSenders, Bloom, ChainSpec, GotExpected, Hardfork, Receipt,
        ReceiptWithBloom, Receipts, Withdrawals, B256,
    },
    providers::{BlockExecutorStats, ProviderError, StateProviderFactory},
    revm::{
        database::StateProviderDatabase,
        eth_dao_fork::{DAO_HARDFORK_BENEFICIARY, DAO_HARDKFORK_ACCOUNTS},
        state_change::post_block_balance_increments,
    },
};
use reth_interfaces::executor::{BlockExecutionError, BlockValidationError};
use reth_node_ethereum::EthEvmConfig;
use std::{sync::Arc, time::Instant};

#[cfg(not(feature = "optimism"))]
use tracing::debug;

use crate::db::{init_builder, SharableStateDBBox};
use crate::traits::{Executable, ParallelBlockExecutor};
use crate::types::NOT_SUPPORT;

/// EVMProcessor is a block executor that uses revm to execute blocks or multiple blocks.
///
/// Output is obtained by calling `take_output_state` function.
///
/// It is capable of pruning the data that will be written to the database
/// and implemented [PrunableBlockExecutor] traits.
///
/// It implemented the [BlockExecutor] that give it the ability to take block
/// apply pre state (Cancun system contract call), execute transaction and apply
/// state change and then apply post execution changes (block reward, withdrawals, irregular DAO
/// hardfork state change). And if `execute_and_verify_receipt` is called it will verify the
/// receipt.
///
/// InspectorStack are used for optional inspecting execution. And it contains
/// various duration of parts of execution.
#[allow(missing_debug_implementations)]
pub struct EVMProcessor<'a, ParallelExecutionModel> {
    /// The configured chain-spec
    pub(crate) chain_spec: Arc<ChainSpec>,

    /// state for parallel execution with multiple EVM instances.
    pub(crate) state: SharableStateDBBox<'a, ProviderError>,

    // pub(crate) state: StateDBBox<'a, ProviderError>,
    pub(crate) execution_model: ParallelExecutionModel,

    // /// revm instance that contains database and env environment.
    // pub(crate) evm: Evm<'a, InspectorStack, StateDBBox<'a, ProviderError>>,
    /// The collection of receipts.
    /// Outer vector stores receipts for each block sequentially.
    /// The inner vector stores receipts ordered by transaction number.
    ///
    /// If receipt is None it means it is pruned.
    pub(crate) receipts: Receipts,
    /// First block will be initialized to `None`
    /// and be set to the block number of first block executed.
    pub(crate) first_block: Option<BlockNumber>,
    /// Execution stats
    pub(crate) stats: BlockExecutorStats,
    /// The type that is able to configure the EVM environment.
    _evm_config: EthEvmConfig,
}

impl<'a, ParallelExecutionModel> EVMProcessor<'a, ParallelExecutionModel>
where
    ParallelExecutionModel: Executable,
{
    /// Return chain spec.
    pub fn chain_spec(&self) -> &Arc<ChainSpec> {
        &self.chain_spec
    }

    /// Create a new pocessor with the given chain spec.
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        state: SharableStateDBBox<'a, ProviderError>,
        execution_model: ParallelExecutionModel,
    ) -> Self {
        // create evm with boxed empty db that is going to be set later.
        // let db = Box::new(
        //     StateBuilder::new().with_database_boxed(Box::new(EmptyDBTyped::<ProviderError>::new())),
        // )
        // .build();

        // // Hook and inspector stack that we want to invoke on that hook.
        // let stack = InspectorStack::new(InspectorStackConfig::default());
        // let evm = evm_config.evm_with_inspector(db, stack);
        EVMProcessor {
            chain_spec,
            execution_model,
            state,
            receipts: Receipts::new(),
            first_block: None,
            stats: BlockExecutorStats::default(),
            _evm_config: EthEvmConfig::default(),
        }
    }

    /// Creates a new executor from the given chain spec and database.
    // pub fn new_with_db<DB: StateProvider + 'a>(
    //     chain_spec: Arc<ChainSpec>,
    //     db: StateProviderDatabase<DB>,
    //     evm_config: EthEvmConfig,
    // ) -> Self {
    //     let state = SharableState::builder()
    //         .with_database_boxed(Box::new(db))
    //         .with_bundle_update()
    //         .without_state_clear()
    //         .build();
    //     EVMProcessor::new_with_state(chain_spec, state, evm_config)
    // }

    /// Create a new EVM processor with the given revm cache state.
    pub fn new_with_db<Client: StateProviderFactory>(
        chain_spec: Arc<ChainSpec>,
        db: &Client,
        evm_config: EthEvmConfig,
    ) -> Self {
        // let stack = InspectorStack::new(InspectorStackConfig::default());
        // let evm = evm_config.evm_with_inspector(revm_state, stack);
        let builder = init_builder();

        EVMProcessor::<ParallelExecutionModel> {
            chain_spec: chain_spec.clone(),
            execution_model: ParallelExecutionModel::new_with_state_provider_factory(
                builder.clone(),
                &chain_spec,
                &evm_config,
                db,
            ),
            state: builder
                .with_database_boxed(Box::new(StateProviderDatabase::new(db.latest().unwrap())))
                .build(),
            receipts: Receipts::new(),
            first_block: None,
            stats: BlockExecutorStats::default(),
            _evm_config: evm_config,
        }
    }

    pub fn new_with_execution_model(
        execution_model: ParallelExecutionModel,
        state: SharableStateDBBox<'a, ProviderError>,
        chain_spec: Arc<ChainSpec>,
        evm_config: EthEvmConfig,
    ) -> Self {
        EVMProcessor::<ParallelExecutionModel> {
            chain_spec: chain_spec.clone(),
            execution_model,
            state,
            receipts: Receipts::new(),
            first_block: None,
            stats: BlockExecutorStats::default(),
            _evm_config: evm_config,
        }
    }

    /// Configure the executor with the given block.
    pub fn set_first_block(&mut self, num: BlockNumber) {
        self.first_block = Some(num);
    }

    /// Returns a reference to the database
    pub fn db_mut(&mut self) -> &'a mut SharableStateDBBox<ProviderError> {
        // &mut self.evm.context.evm.db
        &mut self.state
    }

    /// Execute the block, verify gas usage and apply post-block state changes.
    pub(crate) async fn execute_inner(
        &mut self,
        block: &BlockWithSenders,
    ) -> Result<(Vec<Receipt>, u64), BlockExecutionError> {
        let (receipts, cumulative_gas_used) = self.execution_model.execute(block.clone()).await?;

        //* no need to check header gas limit in OX architecture.
        // // Check if gas used matches the value set in header.
        // if block.gas_used != cumulative_gas_used {
        //     let receipts = Receipts::from_block_receipt(receipts);
        //     return Err(BlockValidationError::BlockGasUsed {
        //         gas: GotExpected {
        //             got: cumulative_gas_used,
        //             expected: block.gas_used,
        //         },
        //         gas_spent_by_tx: receipts.gas_spent_by_tx()?,
        //     }
        //     .into());
        // }
        let time = Instant::now();
        self.apply_post_execution_state_change(block)?;
        self.stats.apply_post_execution_state_changes_duration += time.elapsed();

        //* no need to prune in OX architecture.
        // let time = Instant::now();
        // let retention = if self.tip.map_or(true, |tip| {
        //.     !self
        //         .prune_modes
        //         .account_history
        //         .map_or(false, |mode| mode.should_prune(block.number, tip))
        //         && !self
        //             .prune_modes
        //             .storage_history
        //             .map_or(false, |mode| mode.should_prune(block.number, tip))
        // }) {
        //     BundleRetention::Reverts
        // } else {
        //     BundleRetention::PlainState
        // };
        // self.db_mut().merge_transitions(retention);
        // self.stats.merge_transitions_duration += time.elapsed();

        if self.first_block.is_none() {
            self.first_block = Some(block.number);
        }

        Ok((receipts, cumulative_gas_used))
    }

    /// Apply post execution state changes, including block rewards, withdrawals, and irregular DAO
    /// hardfork state change.
    pub fn apply_post_execution_state_change(
        &mut self,
        block: &Block,
    ) -> Result<(), BlockExecutionError> {
        let mut balance_increments = post_block_balance_increments(
            self.chain_spec(),
            block.number,
            block.difficulty,
            block.beneficiary,
            block.timestamp,
            NOT_SUPPORT,
            &block.ommers,
            block.withdrawals.as_ref().map(Withdrawals::as_ref),
        );

        // Irregular state change at Ethereum DAO hardfork
        if self
            .chain_spec
            .fork(Hardfork::Dao)
            .transitions_at_block(block.number)
        {
            // drain balances from hardcoded addresses.
            let drained_balance: u128 = self
                .state
                .drain_balances(DAO_HARDKFORK_ACCOUNTS)
                .map_err(|_| BlockValidationError::IncrementBalanceFailed)?
                .into_iter()
                .sum();

            // return balance to DAO beneficiary.
            *balance_increments
                .entry(DAO_HARDFORK_BENEFICIARY)
                .or_default() += drained_balance;
        }
        // increment balances

        self.state
            .increment_balances(balance_increments)
            .map_err(|_| BlockValidationError::IncrementBalanceFailed)?;

        Ok(())
    }

    /// Save receipts to the executor.
    pub fn save_receipts(&mut self, receipts: Vec<Receipt>) -> Result<(), BlockExecutionError> {
        // remove receipts of previous blocks
        self.receipts = Receipts::default();

        let receipts = receipts.into_iter().map(Option::Some).collect();
        // // Prune receipts if necessary.
        // self.prune_receipts(&mut receipts)?;
        // Save receipts.
        self.receipts.push(receipts);
        Ok(())
    }
}

/// Default Ethereum implementation of the [ParallelBlockExecutor] trait for the [EVMProcessor].
#[async_trait(?Send)]
impl<'a, ParallelExecutionModel> ParallelBlockExecutor for EVMProcessor<'a, ParallelExecutionModel>
where
    ParallelExecutionModel: Executable,
{
    type Error = BlockExecutionError;

    async fn execute(&mut self, block: &BlockWithSenders) -> Result<(), Self::Error> {
        let (receipts, _) = self.execute_inner(block).await?;
        self.save_receipts(receipts)
    }

    async fn execute_and_verify_receipt(
        &mut self,
        block: &BlockWithSenders,
    ) -> Result<(), Self::Error> {
        // execute block
        let (receipts, _) = self.execute_inner(block).await?;

        // TODO Before Byzantium, receipts contained state root that would mean that expensive
        // operation as hashing that is needed for state root got calculated in every
        // transaction This was replaced with is_success flag.
        // See more about EIP here: https://eips.ethereum.org/EIPS/eip-658
        if self
            .chain_spec
            .fork(Hardfork::Byzantium)
            .active_at_block(block.header.number)
        {
            let time = Instant::now();
            if let Err(error) = verify_receipt(
                block.header.receipts_root,
                block.header.logs_bloom,
                receipts.iter(),
            ) {
                debug!(target: "evm", %error, ?receipts, "receipts verification failed");
                return Err(error);
            };
            self.stats.receipt_root_duration += time.elapsed();
        }

        self.save_receipts(receipts)
    }

    async fn execute_transactions(
        &mut self,
        block: &BlockWithSenders,
    ) -> Result<(Vec<Receipt>, u64), Self::Error> {
        self.execute_inner(block).await
    }

    // async fn execute_transactions(
    //     &mut self,
    //     block: &BlockWithSenders,
    // ) -> Result<(Vec<Receipt>, u64), BlockExecutionError> {
    //     self.init_env(&block.header);

    //     // perf: do not execute empty blocks
    //     if block.body.is_empty() {
    //         return Ok((Vec::new(), 0));
    //     }

    //     let mut cumulative_gas_used = 0;
    //     let mut receipts = Vec::with_capacity(block.body.len());
    //     for (sender, transaction) in block.transactions_with_sender() {
    //         let time = Instant::now();
    //         // The sum of the transaction’s gas limit, Tg, and the gas utilized in this block prior,
    //         // must be no greater than the block’s gasLimit.
    //         let block_available_gas = block.header.gas_limit - cumulative_gas_used;
    //         if transaction.gas_limit() > block_available_gas {
    //             return Err(
    //                 BlockValidationError::TransactionGasLimitMoreThanAvailableBlockGas {
    //                     transaction_gas_limit: transaction.gas_limit(),
    //                     block_available_gas,
    //                 }
    //                 .into(),
    //             );
    //         }
    //         // Execute transaction.
    //         let ResultAndState { result, state } = self.transact(transaction, *sender)?;
    //         trace!(
    //             target: "evm",
    //             ?transaction, ?result, ?state,
    //             "Executed transaction"
    //         );
    //         self.stats.execution_duration += time.elapsed();
    //         let time = Instant::now();

    //         self.db_mut().commit(state);

    //         self.stats.apply_state_duration += time.elapsed();

    //         // append gas used
    //         cumulative_gas_used += result.gas_used();

    //         // Push transaction changeset and calculate header bloom filter for receipt.
    //         receipts.push(Receipt {
    //             tx_type: transaction.tx_type(),
    //             // Success flag was added in `EIP-658: Embedding transaction status code in
    //             // receipts`.
    //             success: result.is_success(),
    //             cumulative_gas_used,
    //             // convert to reth log
    //             logs: result.into_logs().into_iter().map(Into::into).collect(),
    //         });
    //     }

    //     Ok((receipts, cumulative_gas_used))
    // }
}

/// Calculate the receipts root, and copmare it against against the expected receipts root and logs
/// bloom.
pub fn verify_receipt<'a>(
    expected_receipts_root: B256,
    expected_logs_bloom: Bloom,
    receipts: impl Iterator<Item = &'a Receipt> + Clone,
) -> Result<(), BlockExecutionError> {
    // Calculate receipts root.
    let receipts_with_bloom = receipts
        .map(|r| r.clone().into())
        .collect::<Vec<ReceiptWithBloom>>();
    let receipts_root = reth::primitives::proofs::calculate_receipt_root(&receipts_with_bloom);

    // Create header log bloom.
    let logs_bloom = receipts_with_bloom
        .iter()
        .fold(Bloom::ZERO, |bloom, r| bloom | r.bloom);

    compare_receipts_root_and_logs_bloom(
        receipts_root,
        logs_bloom,
        expected_receipts_root,
        expected_logs_bloom,
    )?;

    Ok(())
}

/// Compare the calculated receipts root with the expected receipts root, also copmare
/// the calculated logs bloom with the expected logs bloom.
pub fn compare_receipts_root_and_logs_bloom(
    calculated_receipts_root: B256,
    calculated_logs_bloom: Bloom,
    expected_receipts_root: B256,
    expected_logs_bloom: Bloom,
) -> Result<(), BlockExecutionError> {
    if calculated_receipts_root != expected_receipts_root {
        return Err(BlockValidationError::ReceiptRootDiff(
            GotExpected {
                got: calculated_receipts_root,
                expected: expected_receipts_root,
            }
            .into(),
        )
        .into());
    }

    if calculated_logs_bloom != expected_logs_bloom {
        return Err(BlockValidationError::BloomLogDiff(
            GotExpected {
                got: calculated_logs_bloom,
                expected: expected_logs_bloom,
            }
            .into(),
        )
        .into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db::SharableStateBuilder;
    use crate::types::NOT_SUPPORT;

    use super::*;

    use reth::{
        core::node_config::{ConfigureEvm, ConfigureEvmEnv as _},
        primitives::{
            bytes,
            constants::{BEACON_ROOTS_ADDRESS, EIP1559_INITIAL_BASE_FEE, SYSTEM_ADDRESS},
            keccak256,
            revm::env::fill_tx_env,
            trie::AccountProof,
            Account, Address, Bytecode, Bytes, ChainSpecBuilder, ForkCondition, Header, Signature,
            StorageKey, Transaction, TransactionKind, TransactionSigned, TxEip1559, MAINNET, U256,
        },
        providers::{
            AccountReader, BlockHashReader, BundleStateWithReceipts, StateProvider,
            StateRootProvider,
        },
        revm::{
            db::EmptyDB,
            inspector_handle_register,
            interpreter::Host,
            primitives::{CfgEnvWithHandlerCfg, ResultAndState},
            stack::{InspectorStack, InspectorStackConfig},
            DBBox, Database, DatabaseCommit as _, Evm, Handler, TransitionState,
        },
    };
    use reth_interfaces::provider::ProviderResult;
    use reth_node_ethereum::EthEvmConfig;
    use reth_trie::updates::TrieUpdates;
    use std::collections::HashMap;

    static BEACON_ROOT_CONTRACT_CODE: Bytes = bytes!("3373fffffffffffffffffffffffffffffffffffffffe14604d57602036146024575f5ffd5b5f35801560495762001fff810690815414603c575f5ffd5b62001fff01545f5260205ff35b5f5ffd5b62001fff42064281555f359062001fff015500");

    #[derive(Debug, Default, Clone, Eq, PartialEq)]
    struct StateProviderTest {
        accounts: HashMap<Address, (HashMap<StorageKey, U256>, Account)>,
        contracts: HashMap<B256, Bytecode>,
        block_hash: HashMap<u64, B256>,
    }

    impl StateProviderTest {
        /// Insert account.
        fn insert_account(
            &mut self,
            address: Address,
            mut account: Account,
            bytecode: Option<Bytes>,
            storage: HashMap<StorageKey, U256>,
        ) {
            if let Some(bytecode) = bytecode {
                let hash = keccak256(&bytecode);
                account.bytecode_hash = Some(hash);
                self.contracts.insert(hash, Bytecode::new_raw(bytecode));
            }
            self.accounts.insert(address, (storage, account));
        }
    }

    impl AccountReader for StateProviderTest {
        fn basic_account(&self, address: Address) -> ProviderResult<Option<Account>> {
            Ok(self.accounts.get(&address).map(|(_, acc)| *acc))
        }
    }

    impl BlockHashReader for StateProviderTest {
        fn block_hash(&self, number: u64) -> ProviderResult<Option<B256>> {
            Ok(self.block_hash.get(&number).cloned())
        }

        fn canonical_hashes_range(
            &self,
            start: BlockNumber,
            end: BlockNumber,
        ) -> ProviderResult<Vec<B256>> {
            let range = start..end;
            Ok(self
                .block_hash
                .iter()
                .filter_map(|(block, hash)| range.contains(block).then_some(*hash))
                .collect())
        }
    }

    impl StateRootProvider for StateProviderTest {
        fn state_root(&self, _bundle_state: &BundleStateWithReceipts) -> ProviderResult<B256> {
            unimplemented!("state root computation is not supported")
        }

        fn state_root_with_updates(
            &self,
            _bundle_state: &BundleStateWithReceipts,
        ) -> ProviderResult<(B256, TrieUpdates)> {
            unimplemented!("state root computation is not supported")
        }
    }

    impl StateProvider for StateProviderTest {
        fn storage(
            &self,
            account: Address,
            storage_key: StorageKey,
        ) -> ProviderResult<Option<reth::primitives::StorageValue>> {
            Ok(self
                .accounts
                .get(&account)
                .and_then(|(storage, _)| storage.get(&storage_key).cloned()))
        }

        fn bytecode_by_hash(&self, code_hash: B256) -> ProviderResult<Option<Bytecode>> {
            Ok(self.contracts.get(&code_hash).cloned())
        }

        fn proof(&self, _address: Address, _keys: &[B256]) -> ProviderResult<AccountProof> {
            unimplemented!("proof generation is not supported")
        }
    }

    struct SerialExecutionTest<'a> {
        chain_spec: Arc<ChainSpec>,
        evm: Evm<'a, InspectorStack, SharableStateDBBox<'a, ProviderError>>,
        _evm_config: EthEvmConfig,
    }

    impl<'a> SerialExecutionTest<'a> {
        fn evm(&self) -> &Evm<'a, InspectorStack, SharableStateDBBox<'a, ProviderError>> {
            &self.evm
        }

        fn evm_mut(
            &mut self,
        ) -> &mut Evm<'a, InspectorStack, SharableStateDBBox<'a, ProviderError>> {
            &mut self.evm
        }

        fn init_env(&mut self, header: &Header) {
            // Set state clear flag.
            let state_clear_flag = self
                .chain_spec
                .fork(Hardfork::SpuriousDragon)
                .active_at_block(header.number);

            self.evm
                .context
                .evm
                .db
                .set_state_clear_flag(state_clear_flag);

            let mut cfg: CfgEnvWithHandlerCfg =
                CfgEnvWithHandlerCfg::new_with_spec_id(self.evm.cfg().clone(), self.evm.spec_id());
            EthEvmConfig::fill_cfg_and_block_env(
                &mut cfg,
                self.evm.block_mut(),
                &self.chain_spec,
                header,
                NOT_SUPPORT,
            );
            *self.evm.cfg_mut() = cfg.cfg_env;
            self.evm.handler = Handler::new(cfg.handler_cfg);
        }

        fn transact(
            &mut self,
            transaction: &TransactionSigned,
            sender: Address,
        ) -> Result<ResultAndState, BlockExecutionError> {
            // Fill revm structure.
            fill_tx_env(self.evm_mut().tx_mut(), transaction, sender);

            let hash = transaction.hash();
            let should_inspect = self
                .evm
                .context
                .external
                .should_inspect(self.evm.env(), hash);
            let out = if should_inspect {
                // push inspector handle register.
                self.evm
                    .handler
                    .append_handler_register_plain(inspector_handle_register);
                let output = self.evm.transact();
                // pop last handle register
                self.evm.handler.pop_handle_register();
                output
            } else {
                // Main execution without needing the hash
                self.evm.transact()
            };

            out.map_err(move |e| {
                // Ensure hash is calculated for error log, if not already done
                BlockValidationError::EVM {
                    hash: transaction.recalculate_hash(),
                    error: e.into(), // Convert the error into a boxed error
                }
                .into()
            })
        }
    }

    #[async_trait(?Send)]
    impl<'evm> Executable for SerialExecutionTest<'evm> {
        async fn execute(
            &mut self,
            block: BlockWithSenders,
        ) -> Result<(Vec<Receipt>, u64), BlockExecutionError> {
            reth::revm::state_change::apply_beacon_root_contract_call(
                &self.chain_spec,
                block.timestamp,
                block.number,
                block.parent_beacon_block_root,
                &mut self.evm,
            )?;

            // perf: do not execute empty blocks
            if block.body.is_empty() {
                return Ok((Vec::new(), 0));
            }

            let mut cumulative_gas_used = 0;
            let mut receipts = Vec::with_capacity(block.body.len());
            for (sender, transaction) in block.transactions_with_sender() {
                // The sum of the transaction’s gas limit, Tg, and the gas utilized in this block prior,
                // must be no greater than the block’s gasLimit.
                let block_available_gas = block.header.gas_limit - cumulative_gas_used;
                if transaction.gas_limit() > block_available_gas {
                    return Err(
                        BlockValidationError::TransactionGasLimitMoreThanAvailableBlockGas {
                            transaction_gas_limit: transaction.gas_limit(),
                            block_available_gas,
                        }
                        .into(),
                    );
                }
                // Execute transaction.
                let ResultAndState { result, state } = self.transact(transaction, *sender)?;

                self.evm().context.evm.db.commit(state);

                // append gas used
                cumulative_gas_used += result.gas_used();

                // Push transaction changeset and calculate header bloom filter for receipt.
                receipts.push(Receipt {
                    tx_type: transaction.tx_type(),
                    // Success flag was added in `EIP-658: Embedding transaction status code in
                    // receipts`.
                    success: result.is_success(),
                    cumulative_gas_used,
                    // convert to reth log
                    logs: result.into_logs().into_iter().map(Into::into).collect(),
                });
            }

            Ok((receipts, cumulative_gas_used))
        }

        fn new_with_state_provider_factory<Client: StateProviderFactory>(
            state_builder: SharableStateBuilder<EmptyDB>,
            chain_spec: &Arc<ChainSpec>,
            evm_config: &EthEvmConfig,
            db: &Client,
        ) -> Self {
            Self::new_with_state_provider(
                state_builder,
                chain_spec,
                evm_config,
                Box::new(StateProviderDatabase::new(db.latest().unwrap())),
            )
        }

        fn new_with_state_provider(
            state_builder: SharableStateBuilder<EmptyDB>,
            chain_spec: &Arc<ChainSpec>,
            evm_config: &EthEvmConfig,
            db: DBBox<'static, ProviderError>,
        ) -> Self {
            let stack = InspectorStack::new(InspectorStackConfig::default());

            let state = state_builder.with_database_boxed(db).build();
            Self {
                chain_spec: chain_spec.clone(),
                evm: evm_config.evm_with_inspector(state, stack),
                _evm_config: evm_config.clone(),
            }
        }
    }

    #[tokio::test]
    async fn eip_4788_non_genesis_call() {
        let mut header = Header {
            timestamp: 1,
            number: 1,
            excess_blob_gas: Some(0),
            ..Header::default()
        };

        let mut db: StateProviderTest = StateProviderTest::default();

        let beacon_root_contract_account = Account {
            balance: U256::ZERO,
            bytecode_hash: Some(keccak256(BEACON_ROOT_CONTRACT_CODE.clone())),
            nonce: 1,
        };

        db.insert_account(
            BEACON_ROOTS_ADDRESS,
            beacon_root_contract_account,
            Some(BEACON_ROOT_CONTRACT_CODE.clone()),
            HashMap::new(),
        );

        let chain_spec = Arc::new(
            ChainSpecBuilder::from(&*MAINNET)
                .shanghai_activated()
                .with_fork(Hardfork::Cancun, ForkCondition::Timestamp(1))
                .build(),
        );

        // execute invalid header (no parent beacon block root)
        let builder = init_builder();
        let db_boxed = Box::new(StateProviderDatabase::new(db));
        let execution_model = SerialExecutionTest::new_with_state_provider(
            builder.clone(),
            &chain_spec,
            &EthEvmConfig::default(),
            db_boxed.clone(),
        );
        let mut executor = EVMProcessor::<SerialExecutionTest>::new_with_execution_model(
            execution_model,
            builder.with_database_boxed(db_boxed).build(),
            chain_spec,
            EthEvmConfig::default(),
        );

        // attempt to execute a block without parent beacon block root, expect err
        let err = executor
            .execute_and_verify_receipt(&BlockWithSenders {
                block: Block {
                    header: header.clone(),
                    body: vec![],
                    ommers: vec![],
                    withdrawals: None,
                },
                senders: vec![],
            })
            .await
            .expect_err(
                "Executing cancun block without parent beacon block root field should fail",
            );
        assert_eq!(
            err,
            BlockExecutionError::Validation(BlockValidationError::MissingParentBeaconBlockRoot)
        );

        // fix header, set a gas limit
        header.parent_beacon_block_root = Some(B256::with_last_byte(0x69));

        // Now execute a block with the fixed header, ensure that it does not fail
        executor
            .execute(&BlockWithSenders {
                block: Block {
                    header: header.clone(),
                    body: vec![],
                    ommers: vec![],
                    withdrawals: None,
                },
                senders: vec![],
            })
            .await
            .unwrap();

        // check the actual storage of the contract - it should be:
        // * The storage value at header.timestamp % HISTORY_BUFFER_LENGTH should be
        // header.timestamp
        // * The storage value at header.timestamp % HISTORY_BUFFER_LENGTH + HISTORY_BUFFER_LENGTH
        // should be parent_beacon_block_root
        let history_buffer_length = 8191u64;
        let timestamp_index = header.timestamp % history_buffer_length;
        let parent_beacon_block_root_index =
            timestamp_index % history_buffer_length + history_buffer_length;

        // get timestamp storage and compare
        let timestamp_storage = executor
            .state
            .storage(BEACON_ROOTS_ADDRESS, U256::from(timestamp_index))
            .unwrap();
        assert_eq!(timestamp_storage, U256::from(header.timestamp));

        // get parent beacon block root storage and compare
        let parent_beacon_block_root_storage = executor
            .state
            .storage(
                BEACON_ROOTS_ADDRESS,
                U256::from(parent_beacon_block_root_index),
            )
            .expect("storage value should exist");
        assert_eq!(parent_beacon_block_root_storage, U256::from(0x69));
    }

    #[tokio::test]
    async fn eip_4788_no_code_cancun() {
        // This test ensures that we "silently fail" when cancun is active and there is no code at
        // BEACON_ROOTS_ADDRESS
        let header = Header {
            timestamp: 1,
            number: 1,
            parent_beacon_block_root: Some(B256::with_last_byte(0x69)),
            excess_blob_gas: Some(0),
            ..Header::default()
        };

        // DON'T deploy the contract at genesis
        let chain_spec = Arc::new(
            ChainSpecBuilder::from(&*MAINNET)
                .shanghai_activated()
                .with_fork(Hardfork::Cancun, ForkCondition::Timestamp(1))
                .build(),
        );

        // execute header
        let builder = init_builder();
        let db = Box::new(StateProviderDatabase::new(StateProviderTest::default()));
        let execution_model = SerialExecutionTest::new_with_state_provider(
            builder.clone(),
            &chain_spec,
            &EthEvmConfig::default(),
            db.clone(),
        );
        let mut executor = EVMProcessor::<SerialExecutionTest>::new_with_execution_model(
            execution_model,
            builder.with_database_boxed(db).build(),
            chain_spec,
            EthEvmConfig::default(),
        );
        executor.execution_model.init_env(&header);

        // get the env
        let previous_env = executor.execution_model.evm().context.evm.env.clone();

        // attempt to execute an empty block with parent beacon block root, this should not fail
        executor
            .execute_and_verify_receipt(&BlockWithSenders {
                block: Block {
                    header: header.clone(),
                    body: vec![],
                    ommers: vec![],
                    withdrawals: None,
                },
                senders: vec![],
            })
            .await
            .expect(
                "Executing a block with no transactions while cancun is active should not fail",
            );

        // ensure that the env has not changed
        assert_eq!(executor.execution_model.evm().context.evm.env, previous_env);
    }

    #[tokio::test]
    async fn eip_4788_empty_account_call() {
        // This test ensures that we do not increment the nonce of an empty SYSTEM_ADDRESS account
        // during the pre-block call
        let mut db = StateProviderTest::default();

        let beacon_root_contract_account = Account {
            balance: U256::ZERO,
            bytecode_hash: Some(keccak256(BEACON_ROOT_CONTRACT_CODE.clone())),
            nonce: 1,
        };

        db.insert_account(
            BEACON_ROOTS_ADDRESS,
            beacon_root_contract_account,
            Some(BEACON_ROOT_CONTRACT_CODE.clone()),
            HashMap::new(),
        );

        // insert an empty SYSTEM_ADDRESS
        db.insert_account(SYSTEM_ADDRESS, Account::default(), None, HashMap::new());

        let chain_spec = Arc::new(
            ChainSpecBuilder::from(&*MAINNET)
                .shanghai_activated()
                .with_fork(Hardfork::Cancun, ForkCondition::Timestamp(1))
                .build(),
        );

        // execute header
        let builder = init_builder();
        let db_boxed = Box::new(StateProviderDatabase::new(db));
        let execution_model = SerialExecutionTest::new_with_state_provider(
            builder.clone(),
            &chain_spec,
            &EthEvmConfig::default(),
            db_boxed.clone(),
        );
        let mut executor = EVMProcessor::<SerialExecutionTest>::new_with_execution_model(
            execution_model,
            builder.with_database_boxed(db_boxed).build(),
            chain_spec,
            EthEvmConfig::default(),
        );

        // construct the header for block one
        let header = Header {
            timestamp: 1,
            number: 1,
            parent_beacon_block_root: Some(B256::with_last_byte(0x69)),
            excess_blob_gas: Some(0),
            ..Header::default()
        };

        executor.execution_model.init_env(&header);

        // attempt to execute an empty block with parent beacon block root, this should not fail
        executor
            .execute_and_verify_receipt(&BlockWithSenders {
                block: Block {
                    header: header.clone(),
                    body: vec![],
                    ommers: vec![],
                    withdrawals: None,
                },
                senders: vec![],
            })
            .await
            .expect(
                "Executing a block with no transactions while cancun is active should not fail",
            );

        // ensure that the nonce of the system address account has not changed
        let nonce = executor.state.basic(SYSTEM_ADDRESS).unwrap().unwrap().nonce;
        assert_eq!(nonce, 0);
    }

    #[tokio::test]
    async fn eip_4788_genesis_call() {
        let mut db = StateProviderTest::default();

        let beacon_root_contract_account = Account {
            balance: U256::ZERO,
            bytecode_hash: Some(keccak256(BEACON_ROOT_CONTRACT_CODE.clone())),
            nonce: 1,
        };

        db.insert_account(
            BEACON_ROOTS_ADDRESS,
            beacon_root_contract_account,
            Some(BEACON_ROOT_CONTRACT_CODE.clone()),
            HashMap::new(),
        );

        // activate cancun at genesis
        let chain_spec = Arc::new(
            ChainSpecBuilder::from(&*MAINNET)
                .shanghai_activated()
                .with_fork(Hardfork::Cancun, ForkCondition::Timestamp(0))
                .build(),
        );

        let mut header = chain_spec.genesis_header();

        // execute header
        let builder = init_builder();
        let db_boxed = Box::new(StateProviderDatabase::new(db));
        let execution_model = SerialExecutionTest::new_with_state_provider(
            builder.clone(),
            &chain_spec,
            &EthEvmConfig::default(),
            db_boxed.clone(),
        );
        let mut executor = EVMProcessor::<SerialExecutionTest>::new_with_execution_model(
            execution_model,
            builder.with_database_boxed(db_boxed).build(),
            chain_spec,
            EthEvmConfig::default(),
        );
        executor.execution_model.init_env(&header);

        // attempt to execute the genesis block with non-zero parent beacon block root, expect err
        header.parent_beacon_block_root = Some(B256::with_last_byte(0x69));
        let _err = executor
            .execute_and_verify_receipt(
                &BlockWithSenders {
                    block: Block {
                        header: header.clone(),
                        body: vec![],
                        ommers: vec![],
                        withdrawals: None,
                    },
                    senders: vec![],
                }
            ).await
            .expect_err(
                "Executing genesis cancun block with non-zero parent beacon block root field should fail",
            );

        // fix header
        header.parent_beacon_block_root = Some(B256::ZERO);

        // now try to process the genesis block again, this time ensuring that a system contract
        // call does not occur
        executor
            .execute(&BlockWithSenders {
                block: Block {
                    header: header.clone(),
                    body: vec![],
                    ommers: vec![],
                    withdrawals: None,
                },
                senders: vec![],
            })
            .await
            .unwrap();

        // there is no system contract call so there should be NO STORAGE CHANGES
        // this means we'll check the transition state
        let state = &executor.execution_model.evm().context.evm.db;
        let transition_state = state
            .transition_state
            .clone()
            .expect("the evm should be initialized with bundle updates");

        // assert that it is the default (empty) transition state
        assert!(transition_state.read().eq(&TransitionState::default()));
    }

    #[tokio::test]
    async fn eip_4788_high_base_fee() {
        // This test ensures that if we have a base fee, then we don't return an error when the
        // system contract is called, due to the gas price being less than the base fee.
        let header = Header {
            timestamp: 1,
            number: 1,
            parent_beacon_block_root: Some(B256::with_last_byte(0x69)),
            base_fee_per_gas: Some(u64::MAX),
            excess_blob_gas: Some(0),
            ..Header::default()
        };

        let mut db = StateProviderTest::default();

        let beacon_root_contract_account = Account {
            balance: U256::ZERO,
            bytecode_hash: Some(keccak256(BEACON_ROOT_CONTRACT_CODE.clone())),
            nonce: 1,
        };

        db.insert_account(
            BEACON_ROOTS_ADDRESS,
            beacon_root_contract_account,
            Some(BEACON_ROOT_CONTRACT_CODE.clone()),
            HashMap::new(),
        );

        let chain_spec = Arc::new(
            ChainSpecBuilder::from(&*MAINNET)
                .shanghai_activated()
                .with_fork(Hardfork::Cancun, ForkCondition::Timestamp(1))
                .build(),
        );

        // execute header
        let builder = init_builder();
        let boxed_db = Box::new(StateProviderDatabase::new(db));
        let execution_model = SerialExecutionTest::new_with_state_provider(
            builder.clone(),
            &chain_spec,
            &EthEvmConfig::default(),
            boxed_db.clone(),
        );
        let mut executor = EVMProcessor::<SerialExecutionTest>::new_with_execution_model(
            execution_model,
            builder.with_database_boxed(boxed_db.clone()).build(),
            chain_spec,
            EthEvmConfig::default(),
        );
        executor.execution_model.init_env(&header);

        // ensure that the env is configured with a base fee
        assert_eq!(
            executor.execution_model.evm().block().basefee,
            U256::from(u64::MAX)
        );

        // Now execute a block with the fixed header, ensure that it does not fail
        executor
            .execute(&BlockWithSenders {
                block: Block {
                    header: header.clone(),
                    body: vec![],
                    ommers: vec![],
                    withdrawals: None,
                },
                senders: vec![],
            })
            .await
            .unwrap();

        // check the actual storage of the contract - it should be:
        // * The storage value at header.timestamp % HISTORY_BUFFER_LENGTH should be
        // header.timestamp
        // * The storage value at header.timestamp % HISTORY_BUFFER_LENGTH + HISTORY_BUFFER_LENGTH
        // should be parent_beacon_block_root
        let history_buffer_length = 8191u64;
        let timestamp_index = header.timestamp % history_buffer_length;
        let parent_beacon_block_root_index =
            timestamp_index % history_buffer_length + history_buffer_length;

        // get timestamp storage and compare
        let timestamp_storage = executor
            .state
            .storage(BEACON_ROOTS_ADDRESS, U256::from(timestamp_index))
            .unwrap();
        assert_eq!(timestamp_storage, U256::from(header.timestamp));

        // get parent beacon block root storage and compare
        let parent_beacon_block_root_storage = executor
            .state
            .storage(
                BEACON_ROOTS_ADDRESS,
                U256::from(parent_beacon_block_root_index),
            )
            .unwrap();
        assert_eq!(parent_beacon_block_root_storage, U256::from(0x69));
    }

    #[test]
    fn test_transact_error_includes_correct_hash() {
        let chain_spec = Arc::new(
            ChainSpecBuilder::from(&*MAINNET)
                .shanghai_activated()
                .with_fork(Hardfork::Cancun, ForkCondition::Timestamp(1))
                .build(),
        );

        let chain_id = chain_spec.chain.id();

        // execute header
        let mut executor = SerialExecutionTest::new_with_state_provider(
            init_builder(),
            &chain_spec,
            &EthEvmConfig::default(),
            Box::new(StateProviderDatabase::new(StateProviderTest::default())),
        );

        // Create a test transaction that gonna fail
        let transaction = TransactionSigned::from_transaction_and_signature(
            Transaction::Eip1559(TxEip1559 {
                chain_id,
                nonce: 1,
                gas_limit: 21_000,
                to: TransactionKind::Call(Address::ZERO),
                max_fee_per_gas: EIP1559_INITIAL_BASE_FEE as u128,
                ..Default::default()
            }),
            Signature::default(),
        );

        let result = executor.transact(&transaction, Address::random());

        let expected_hash = transaction.recalculate_hash();

        // Check the error
        match result {
            Err(BlockExecutionError::Validation(BlockValidationError::EVM { hash, error: _ })) => {
                    assert_eq!(hash, expected_hash, "The EVM error does not include the correct transaction hash.");
            },
            _ => panic!("Expected a BlockExecutionError::Validation error, but transaction did not fail as expected."),
        }
    }
}
