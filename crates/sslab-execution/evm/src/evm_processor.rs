use async_trait::async_trait;
use reth::{
    primitives::{
        Block, BlockNumber, BlockWithSenders, Bloom, ChainSpec, GotExpected, Hardfork, Receipt,
        ReceiptWithBloom, Receipts, Withdrawals, B256,
    },
    providers::{BlockExecutorStats, BundleStateWithReceipts, ProviderError},
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

use crate::traits::{Executable, ParallelBlockExecutor};
use crate::types::NOT_SUPPORT;
use crate::{
    db::{SharableState, SharableStateDBBox, ThreadSafeCacheState},
    ProviderFactoryMDBX,
};

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
    pub fn new(provider_factory: ProviderFactoryMDBX, chain_spec: Arc<ChainSpec>) -> Self {
        let cached_state = ThreadSafeCacheState::default();
        let state = SharableState::builder()
            .with_database_boxed(Box::new(StateProviderDatabase::new(
                provider_factory.latest().unwrap(),
            )))
            .with_cached_prestate(cached_state.clone())
            .with_bundle_update()
            .build();

        EVMProcessor {
            execution_model: ParallelExecutionModel::new_with_db(
                provider_factory.clone(),
                Some(cached_state),
                chain_spec.clone(),
            ),
            chain_spec,
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

    /// Create a new EVM processor with the given db, without preset cached state.
    // pub fn new_with_db<Client: StateProviderFactory>(
    //     chain_spec: Arc<ChainSpec>,
    //     db: Client,
    //     evm_config: EthEvmConfig,
    // ) -> Self {
    //     // let stack = InspectorStack::new(InspectorStackConfig::default());
    //     // let evm = evm_config.evm_with_inspector(revm_state, stack);

    //     let cache_state = ThreadSafeCacheState::default();
    //     let state = SharableState::builder()
    //         .with_database_boxed(Box::new(StateProviderDatabase::new(
    //             (&db).latest().unwrap(),
    //         )))
    //         .with_cached_prestate(cache_state.clone())
    //         .with_bundle_update()
    //         .build();

    //     EVMProcessor::<ParallelExecutionModel> {
    //         chain_spec: chain_spec.clone(),
    //         execution_model: ParallelExecutionModel::new_with_cached_state(
    //             cache_state,
    //             &chain_spec,
    //             &evm_config,
    //             db,
    //         ),
    //         state,
    //         receipts: Receipts::new(),
    //         first_block: None,
    //         stats: BlockExecutorStats::default(),
    //         _evm_config: evm_config,
    //     }
    // }

    /// Configure the executor with the given block.
    pub fn set_first_block(&mut self, num: BlockNumber) {
        self.first_block = Some(num);
    }

    /// Returns a reference to the database
    // pub fn db_mut(&mut self) -> &'a mut SharableStateDBBox<ProviderError> {
    //     // &mut self.evm.context.evm.db
    //     &mut self.state
    // }

    /// Execute the block, verify gas usage and apply post-block state changes.
    pub(crate) async fn execute_inner(
        &mut self,
        block: BlockWithSenders,
    ) -> Result<(BlockWithSenders, Vec<Receipt>, u64), BlockExecutionError> {
        let (new_block, receipts, cumulative_gas_used) =
            self.execution_model.execute(block).await?;

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
        self.apply_post_execution_state_change(&new_block.block)?;
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
            self.first_block = Some(new_block.block.number);
        }

        Ok((new_block, receipts, cumulative_gas_used))
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
        let receipts = Receipts {
            receipt_vec: vec![receipts.into_iter().map(Option::Some).collect()],
        };
        // // Prune receipts if necessary.
        // self.prune_receipts(&mut receipts)?;
        // Save receipts.
        let _ = std::mem::replace(&mut self.receipts, receipts);

        Ok(())
    }

    pub fn take_output_state(&mut self) -> BundleStateWithReceipts {
        self.stats.log_debug();
        let receipts = std::mem::take(&mut self.receipts);

        BundleStateWithReceipts::new(
            self.state.take_bundle(),
            receipts,
            self.first_block.unwrap_or_default(),
        )
    }
}

/// Default Ethereum implementation of the [ParallelBlockExecutor] trait for the [EVMProcessor].
#[async_trait(?Send)]
impl<'a, ParallelExecutionModel> ParallelBlockExecutor for EVMProcessor<'a, ParallelExecutionModel>
where
    ParallelExecutionModel: Executable,
{
    type Error = BlockExecutionError;

    async fn execute(&mut self, block: BlockWithSenders) -> Result<(), Self::Error> {
        let (_, receipts, _) = self.execute_inner(block).await?;
        self.save_receipts(receipts)
    }

    async fn execute_and_verify_receipt(
        &mut self,
        block: BlockWithSenders,
    ) -> Result<(), Self::Error> {
        // execute block
        let (new_block, receipts, _) = self.execute_inner(block).await?;

        // TODO Before Byzantium, receipts contained state root that would mean that expensive
        // operation as hashing that is needed for state root got calculated in every
        // transaction This was replaced with is_success flag.
        // See more about EIP here: https://eips.ethereum.org/EIPS/eip-658
        if self
            .chain_spec
            .fork(Hardfork::Byzantium)
            .active_at_block(new_block.header.number)
        {
            let time = Instant::now();
            if let Err(error) = verify_receipt(
                new_block.header.receipts_root,
                new_block.header.logs_bloom,
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
        block: BlockWithSenders,
    ) -> Result<(BlockWithSenders, Vec<Receipt>, u64), BlockExecutionError> {
        self.execution_model.execute(block).await
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
