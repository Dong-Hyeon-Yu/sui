use std::sync::Arc;

use async_trait::async_trait;
use reth::{
    primitives::{BlockWithSenders, ChainSpec, Receipt},
    providers::{ProviderError, StateProviderFactory},
    revm::{db::EmptyDB, DBBox},
};
use reth_interfaces::executor::BlockExecutionError;
use reth_node_ethereum::EthEvmConfig;

use crate::db::SharableStateBuilder;

#[async_trait(?Send)]
pub trait Executable {
    async fn execute(
        &mut self,
        consensus_output: BlockWithSenders,
    ) -> Result<(Vec<Receipt>, u64), BlockExecutionError>;

    fn new_with_state_provider_factory<Client: StateProviderFactory>(
        state_builder: SharableStateBuilder<EmptyDB>,
        chain_spec: &Arc<ChainSpec>,
        evm_config: &EthEvmConfig,
        db: &Client,
    ) -> Self;

    fn new_with_state_provider(
        state_builder: SharableStateBuilder<EmptyDB>,
        chain_spec: &Arc<ChainSpec>,
        evm_config: &EthEvmConfig,
        db: DBBox<'static, ProviderError>,
    ) -> Self;
}

/// An abstraction for an executor in a sui PrimaryNode.
#[async_trait(?Send)]
pub(crate) trait ExecutionComponent {
    async fn run(&mut self);
}

/// An executor capable of executing a block in parallel.
#[async_trait(?Send)]
pub(crate) trait ParallelBlockExecutor {
    /// The error type returned by the executor.
    type Error;

    /// Execute a block.
    async fn execute(&mut self, block: &BlockWithSenders) -> Result<(), Self::Error>;

    /// Executes the block and checks receipts.
    ///
    /// See [execute](BlockExecutor::execute) for more details.
    async fn execute_and_verify_receipt(
        &mut self,
        block: &BlockWithSenders,
    ) -> Result<(), Self::Error>;

    /// Runs the provided transactions and commits their state to the run-time database.
    ///
    /// The returned [BundleStateWithReceipts] can be used to persist the changes to disk, and
    /// contains the changes made by each transaction.
    ///
    /// The changes in [BundleStateWithReceipts] have a transition ID associated with them: there is
    /// one transition ID for each transaction (with the first executed tx having transition ID
    /// 0, and so on).
    ///
    /// The second returned value represents the total gas used by this block of transactions.
    ///
    /// See [execute](BlockExecutor::execute) for more details.
    async fn execute_transactions(
        &mut self,
        block: &BlockWithSenders,
    ) -> Result<(Vec<Receipt>, u64), Self::Error>;

    // /// Return bundle state. This is output of executed blocks.
    // async fn take_output_state(&mut self) -> BundleStateWithReceipts;

    // /// Returns the size hint of current in-memory changes.
    // fn size_hint(&self) -> Option<usize>;
}
