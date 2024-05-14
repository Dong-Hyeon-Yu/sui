use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use reth::{
    primitives::{
        constants::{EMPTY_TRANSACTIONS, ETHEREUM_BLOCK_GAS_LIMIT},
        proofs, Block, BlockBody, BlockWithSenders, ChainSpec, Header, SealedHeader,
        TransactionSigned, B256, EMPTY_OMMER_ROOT_HASH, U256,
    },
    providers::{BlockReaderIdExt, BundleStateWithReceipts, StateProviderFactory},
    revm::db::states::bundle_state::BundleRetention,
};
use reth_interfaces::executor::{BlockExecutionError, BlockValidationError};
use reth_node_ethereum::EthEvmConfig;
use tokio::sync::mpsc::Receiver;
use tracing::{debug, trace};

use crate::{
    evm_processor::EVMProcessor,
    revm_utiles::_unpack_batches,
    traits::{Executable, ExecutionComponent, ParallelBlockExecutor as _},
    types::ExecutableConsensusOutput,
};

/// Client is [reth::provider::BlockchainProvider].
pub struct ParallelExecutor<ParallelExecutionModel, Client> {
    rx_consensus_certificate: Receiver<ExecutableConsensusOutput>,

    // rx_shutdown: ConditionalBroadcastReceiver,
    inner: Inner<ParallelExecutionModel, Client>,
}

#[async_trait(?Send)]
impl<ParallelExecutionModel, Client> ExecutionComponent
    for ParallelExecutor<ParallelExecutionModel, Client>
where
    ParallelExecutionModel: Executable + Send + Sync + Default,
    Client: BlockReaderIdExt + StateProviderFactory,
{
    async fn run(&mut self) {
        while let Some(consensus_output) = self.rx_consensus_certificate.recv().await {
            debug!(
                "Received consensus output at leader round {}, subdag index {}, timestamp {} ",
                consensus_output.round(),
                consensus_output.sub_dag_index(),
                consensus_output.timestamp(),
            );
            cfg_if::cfg_if! {
                if #[cfg(feature = "benchmark")] {
                    use tracing::info;
                    // NOTE: This log entry is used to compute performance.
                    consensus_output.data().iter().for_each(|batch_digest|
                        info!("Received Batch -> {:?}", batch_digest.digest())
                    );
                }
            }

            let (_digests, transactions) = _unpack_batches(consensus_output.take_data()).await;
            let _ = self.inner.build_and_execute(transactions).await;

            cfg_if::cfg_if! {
                if #[cfg(feature = "benchmark")] {
                    // NOTE: This log entry is used to compute performance.
                    _digests.iter().for_each(|batch_digest|
                        info!("Executed Batch -> {:?}", batch_digest)
                    );
                }
            }
        }
    }
}

impl<ParallelExecutionModel, Client> ParallelExecutor<ParallelExecutionModel, Client>
where
    ParallelExecutionModel: Executable + Send + Sync + Default,
    Client: BlockReaderIdExt + StateProviderFactory,
{
    pub fn new(
        rx_consensus_certificate: Receiver<ExecutableConsensusOutput>,
        // rx_shutdown: ConditionalBroadcastReceiver,
        chain_spec: Arc<ChainSpec>,
        client: Client,
        evm_config: EthEvmConfig,
    ) -> Self {
        Self {
            rx_consensus_certificate,
            // rx_shutdown,
            inner: Inner::new(chain_spec, client, evm_config),
        }
    }
}

pub struct Inner<ParallelExecutionModel, Client> {
    pub(crate) latest: Header,
    pub(crate) latest_hash: B256,

    pub(crate) chain_spec: Arc<ChainSpec>,

    pub(crate) client: Client,

    pub(crate) executor: EVMProcessor<'static, ParallelExecutionModel>,
}

impl<ParallelExecutionModel, Client> Inner<ParallelExecutionModel, Client>
where
    ParallelExecutionModel: Executable + Send + Sync + Default,
    Client: BlockReaderIdExt + StateProviderFactory,
{
    pub fn new(chain_spec: Arc<ChainSpec>, client: Client, evm_config: EthEvmConfig) -> Self {
        let best_header = client
            .latest_header()
            .ok()
            .flatten()
            .unwrap_or_else(|| chain_spec.sealed_genesis_header());

        let (header, best_hash) = best_header.split();
        let executor = EVMProcessor::<ParallelExecutionModel>::new_with_db(
            chain_spec.clone(),
            &client,
            evm_config,
        );

        Self {
            latest: header,
            latest_hash: best_hash,
            chain_spec,
            client,
            executor,
        }
    }

    /// Inserts a new header+body pair
    pub(crate) fn record_new_block(&mut self, header: Header) {
        self.latest = header;
        self.latest_hash = self.latest.hash_slow();

        trace!(target: "consensus::auto", num=self.latest.number, hash=?self.latest_hash, "inserting new block");
    }

    /// Fills in pre-execution header fields based on the current best block and given
    /// transactions.
    pub(crate) fn build_header_template(
        &self,
        transactions: &[TransactionSigned],
        chain_spec: Arc<ChainSpec>,
    ) -> Header {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // check previous block for base fee
        let base_fee_per_gas = self
            .latest
            .next_block_base_fee(chain_spec.base_fee_params(timestamp));

        let mut header = Header {
            parent_hash: self.latest_hash,
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            beneficiary: Default::default(),
            state_root: Default::default(),
            transactions_root: Default::default(),
            receipts_root: Default::default(),
            withdrawals_root: None,
            logs_bloom: Default::default(),
            difficulty: U256::from(2),
            number: self.latest.number + 1,
            gas_limit: ETHEREUM_BLOCK_GAS_LIMIT,
            gas_used: 0,
            timestamp,
            mix_hash: Default::default(),
            nonce: 0,
            base_fee_per_gas,
            blob_gas_used: None,
            excess_blob_gas: None,
            extra_data: Default::default(),
            parent_beacon_block_root: None,
        };

        header.transactions_root = if transactions.is_empty() {
            EMPTY_TRANSACTIONS
        } else {
            proofs::calculate_transaction_root(transactions)
        };

        header
    }

    /// Executes the block with the given block and senders, on the provided [EVMProcessor].
    ///
    /// This returns the poststate from execution and post-block changes, as well as the gas used.
    pub(crate) async fn execute_with_block(
        &mut self,
        block: &mut BlockWithSenders,
    ) -> Result<(BundleStateWithReceipts, u64), BlockExecutionError> {
        trace!(target: "consensus::auto", transactions=?&block.body, "executing transactions");
        // TODO: there isn't really a parent beacon block root here, so not sure whether or not to
        // call the 4788 beacon contract

        // set the first block to find the correct index in bundle state
        self.executor.set_first_block(block.number);

        let (receipts, gas_used) = self.executor.execute_transactions(block).await?;

        if !block.body.is_empty() {
            block.block.header.transactions_root =
                proofs::calculate_transaction_root(block.body.as_slice());
        }

        // Save receipts.
        self.executor.save_receipts(receipts)?;

        // add post execution state change
        // Withdrawals, rewards etc.
        self.executor.apply_post_execution_state_change(block)?;

        // merge transitions
        self.executor
            .state
            .merge_transitions(BundleRetention::Reverts);

        // apply post block changes
        Ok((BundleStateWithReceipts::default(), gas_used))
    }

    /// Fills in the post-execution header fields based on the given BundleState and gas used.
    /// In doing this, the state root is calculated and the final header is returned.
    #[cfg(not(feature = "optimism"))]
    pub(crate) fn complete_header<S: StateProviderFactory>(
        &self,
        mut header: Header,
        bundle_state: &BundleStateWithReceipts,
        client: &S,
        gas_used: u64,
    ) -> Result<Header, BlockExecutionError> {
        use reth::primitives::{constants::EMPTY_RECEIPTS, Bloom, ReceiptWithBloom};

        let receipts = bundle_state.receipts_by_block(header.number);
        header.receipts_root = if receipts.is_empty() {
            EMPTY_RECEIPTS
        } else {
            let receipts_with_bloom = receipts
                .iter()
                .map(|r| (*r).clone().expect("receipts have not been pruned").into())
                .collect::<Vec<ReceiptWithBloom>>();
            header.logs_bloom = receipts_with_bloom
                .iter()
                .fold(Bloom::ZERO, |bloom, r| bloom | r.bloom);
            proofs::calculate_receipt_root(&receipts_with_bloom)
        };

        header.gas_used = gas_used;

        // calculate the state root
        let state_root = client
            .latest()
            .map_err(|_| BlockExecutionError::ProviderError)?
            .state_root(bundle_state)
            .unwrap();
        header.state_root = state_root;
        Ok(header)
    }

    /// Builds and executes a new block with the given transactions, on the provided [EVMProcessor].
    ///
    /// This returns the header of the executed block, as well as the poststate from execution.
    pub(crate) async fn build_and_execute(
        &mut self,
        transactions: Vec<TransactionSigned>,
    ) -> Result<(SealedHeader, BundleStateWithReceipts), BlockExecutionError> {
        let header = self.build_header_template(&transactions, self.chain_spec.clone());

        let mut block = Block {
            header,
            body: transactions,
            ommers: vec![],
            withdrawals: None,
        }
        .with_recovered_senders()
        .ok_or(BlockExecutionError::Validation(
            BlockValidationError::SenderRecoveryError,
        ))?;

        trace!(target: "consensus::auto", transactions=?&block.body, "executing transactions");

        // now execute the block
        let (bundle_state, gas_used) = self.execute_with_block(&mut block).await?;

        let Block { header, body, .. } = block.block;
        let body = BlockBody {
            transactions: body,
            ommers: vec![],
            withdrawals: None,
        };

        trace!(target: "consensus::auto", ?bundle_state, ?header, ?body, "executed block, calculating state root and completing header");

        // fill in the rest of the fields
        let header = self.complete_header(header, &bundle_state, &self.client, gas_used)?;

        trace!(target: "consensus::auto", root=?header.state_root, ?body, "calculated root");

        // finally insert into storage
        self.record_new_block(header.clone());

        // set new header with hash that should have been updated by insert_new_block
        let new_header = header.seal(self.latest_hash);

        Ok((new_header, bundle_state))
    }
}
