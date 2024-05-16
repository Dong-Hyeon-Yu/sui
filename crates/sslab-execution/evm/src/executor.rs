use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use reth::{
    blockchain_tree::noop::NoopBlockchainTree,
    primitives::{
        constants::ETHEREUM_BLOCK_GAS_LIMIT, proofs, Block, BlockWithSenders, ChainSpec, Header,
        SealedBlock, SealedHeader, TransactionSigned, B256, EMPTY_OMMER_ROOT_HASH, U256,
    },
    providers::{
        providers::BlockchainProvider, BlockReaderIdExt, BundleStateWithReceipts,
        DatabaseProviderRW, ProviderError,
    },
    revm::db::states::bundle_state::BundleRetention,
};

use reth_db::{
    cursor::{DbCursorRO, DbCursorRW},
    database::Database,
    models::{StoredBlockBodyIndices, StoredBlockOmmers, StoredBlockWithdrawals},
    tables,
    transaction::DbTxMut,
    DatabaseEnv, DatabaseError,
};
use reth_interfaces::executor::{BlockExecutionError, BlockValidationError};

use tokio::sync::mpsc::Receiver;
use tracing::{debug, trace};

use crate::{
    evm_processor::EVMProcessor,
    get_provider_factory_rw,
    revm_utiles::unpack_batches,
    traits::{Executable, ExecutionComponent, ParallelBlockExecutor as _},
    types::ExecutableConsensusOutput,
    ProviderFactoryMDBX,
};

/// Client is [reth::provider::BlockchainProvider].
pub struct ParallelExecutor<ParallelExecutionModel> {
    rx_consensus_certificate: Receiver<ExecutableConsensusOutput>,

    // rx_shutdown: ConditionalBroadcastReceiver,
    inner: Inner<ParallelExecutionModel>,
}

#[async_trait(?Send)]
impl<ParallelExecutionModel: Executable> ExecutionComponent
    for ParallelExecutor<ParallelExecutionModel>
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

            let (_digests, transactions) = unpack_batches(consensus_output.take_data()).await;
            let _ = self.inner.execute_and_persist(transactions).await;

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

impl<ParallelExecutionModel: Executable> ParallelExecutor<ParallelExecutionModel> {
    pub fn new(
        rx_consensus_certificate: Receiver<ExecutableConsensusOutput>,
        // rx_shutdown: ConditionalBroadcastReceiver,
        chain_spec: Arc<ChainSpec>,
    ) -> Self {
        Self {
            rx_consensus_certificate,
            // rx_shutdown,
            inner: Inner::new(get_provider_factory_rw(chain_spec.clone()), chain_spec),
        }
    }
}

pub struct Inner<ParallelExecutionModel> {
    pub(crate) latest: Header,
    pub(crate) latest_hash: B256,

    pub(crate) chain_spec: Arc<ChainSpec>,

    pub(crate) db: ProviderFactoryMDBX,

    // pub(crate) provider_factory: ProviderFactoryMDBX,
    pub(crate) executor: EVMProcessor<'static, ParallelExecutionModel>,
}

impl<ParallelExecutionModel: Executable> Inner<ParallelExecutionModel> {
    pub fn new(factory: ProviderFactoryMDBX, chain_spec: Arc<ChainSpec>) -> Self {
        let client =
            BlockchainProvider::new(factory.clone(), NoopBlockchainTree::default()).unwrap();

        let best_header = client
            .latest_header()
            .ok()
            .flatten()
            .unwrap_or_else(|| chain_spec.sealed_genesis_header());

        let (header, best_hash) = best_header.split();

        Self {
            latest: header,
            latest_hash: best_hash,
            chain_spec: chain_spec.clone(),
            db: factory.clone(),
            executor: EVMProcessor::<ParallelExecutionModel>::new(factory, chain_spec),
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
        // transactions: &[TransactionSigned],
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

        Header {
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
        }

        // header.transactions_root = if transactions.is_empty() {
        //     EMPTY_TRANSACTIONS
        // } else {
        //     proofs::calculate_transaction_root(transactions)
        // };

        // header
    }

    /// Executes the block with the given block and senders, on the provided [EVMProcessor].
    ///
    /// This returns the poststate from execution and post-block changes, as well as the gas used.
    pub(crate) async fn execute(
        &mut self,
        block: BlockWithSenders,
    ) -> Result<(BlockWithSenders, BundleStateWithReceipts, u64), BlockExecutionError> {
        trace!(target: "consensus::auto", transactions=?&block.body, "executing transactions");
        // TODO: there isn't really a parent beacon block root here, so not sure whether or not to
        // call the 4788 beacon contract

        // set the first block to find the correct index in bundle state
        self.executor.set_first_block(block.number - 1);

        let (mut new_block, receipts, gas_used) = self.executor.execute_transactions(block).await?;

        if !new_block.body.is_empty() {
            new_block.block.header.transactions_root =
                proofs::calculate_transaction_root(new_block.body.as_slice());
        }

        // Save receipts.
        self.executor.save_receipts(receipts)?;

        // add post execution state change
        // Withdrawals, rewards etc.
        //* No mining reward or withdrawals in OX architecture!
        // self.executor.apply_post_execution_state_change(block)?;

        // merge transitions
        self.executor
            .state
            .merge_transitions(BundleRetention::Reverts);

        // apply post block changes
        Ok((new_block, self.executor.take_output_state(), gas_used))
    }

    /// Fills in the post-execution header fields based on the given BundleState and gas used.
    /// In doing this, the state root is calculated and the final header is returned.
    pub(crate) fn complete_header(
        &self,
        mut header: Header,
        bundle_state: &BundleStateWithReceipts,
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
        let state_root = self
            .db
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
    pub async fn execute_and_persist(
        &mut self,
        transactions: Vec<TransactionSigned>,
    ) -> Result<(), BlockExecutionError> {
        let header = self.build_header_template(self.chain_spec.clone());

        let block = Block {
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
        let (new_block, bundle_state, gas_used) = self.execute(block).await?;

        let BlockWithSenders { block, senders: _ } = new_block;
        let Block { header, body, .. } = block;

        trace!(target: "consensus::auto", ?bundle_state, ?header, ?body, "executed block, calculating state root and completing header");

        // fill in the rest of the fields
        let header = self.complete_header(header, &bundle_state, gas_used)?;

        trace!(target: "consensus::auto", root=?header.state_root, ?body, "calculated root");

        // finally insert into storage
        self.record_new_block(header.clone());

        // set new header with hash that should have been updated by insert_new_block
        let new_header = header.seal(self.latest_hash);

        self.persist(new_header, body, bundle_state)
            .await
            .map_err(|e| BlockExecutionError::CanonicalCommit {
                inner: e.to_string(),
            })?;

        Ok(())
    }

    pub(crate) async fn persist(
        &self,
        header: SealedHeader,
        body: Vec<TransactionSigned>,
        bundle_state: BundleStateWithReceipts,
    ) -> Result<(), ProviderError> {
        // seal the block
        let block = Block {
            header: header.clone().unseal(),
            body,
            ommers: vec![],
            withdrawals: None,
        };
        let sealed_block = block.seal_slow();

        // let tx_bundle_state = self.db.provider_rw()?;
        // let tx_header = self.db.provider_rw()?;
        // let tx_body = self.db.provider_rw()?;

        let db = self.db.provider_rw()?;

        bundle_state.write_to_db(db.tx_ref(), reth::providers::OriginalValuesKnown::Yes)?;
        write_header(db.tx_ref(), header)?;
        write_block(&db, sealed_block)?;
        db.commit()?;

        // let handles = Vec::from([
        //     tokio::spawn(async move {
        //         println!("writing bundle state");
        //         match bundle_state.write_to_db(
        //             tx_bundle_state.tx_ref(),
        //             reth::providers::OriginalValuesKnown::Yes,
        //         ) {
        //             Ok(_) => tx_bundle_state.commit(),
        //             Err(e) => Err(e.into()),
        //         }
        //     }),
        //     tokio::spawn(async move {
        //         println!("writing header");
        //         match write_header(tx_header.tx_ref(), header) {
        //             Ok(_) => tx_header.commit(),
        //             Err(e) => Err(e.into()),
        //         }
        //     }),
        //     tokio::spawn(async move {
        //         println!("writing block");
        //         match write_block(&tx_body, sealed_block) {
        //             Ok(_) => tx_body.commit(),
        //             Err(e) => Err(e.into()),
        //         }
        //     }),
        // ]);

        // for res in try_join_all(handles).await.unwrap() {
        //     res?;
        // }

        Ok(())
    }
}

fn write_header(
    tx: &<DatabaseEnv as Database>::TXMut,
    header: SealedHeader,
) -> Result<(), DatabaseError> {
    // trace!(target: "sync::stages::headers", len = header.hash(), "writing header");

    let mut cursor_header = tx.cursor_write::<tables::Headers>()?;
    let mut cursor_canonical = tx.cursor_write::<tables::CanonicalHeaders>()?;

    if header.number == 0 {
        return Ok(());
    }

    let header_hash = header.hash();
    let header_number = header.number;
    let header = header.unseal();

    // NOTE: HeaderNumbers are not sorted and can't be inserted with cursor.
    tx.put::<tables::HeaderNumbers>(header_hash, header_number)?;
    cursor_header.insert(header_number, header)?;
    cursor_canonical.insert(header_number, header_hash)?;

    Ok(())
}

fn write_block<DB: Database>(
    provider: &DatabaseProviderRW<DB>,
    block: SealedBlock,
) -> Result<(), DatabaseError> {
    // Cursors used to write bodies, ommers and transactions
    let tx = provider.tx_ref();
    let mut block_indices_cursor = tx.cursor_write::<tables::BlockBodyIndices>()?;
    let mut tx_cursor = tx.cursor_write::<tables::Transactions>()?;
    let mut tx_block_cursor = tx.cursor_write::<tables::TransactionBlock>()?;
    let mut ommers_cursor = tx.cursor_write::<tables::BlockOmmers>()?;
    let mut withdrawals_cursor = tx.cursor_write::<tables::BlockWithdrawals>()?;

    // Get id for the next tx_num or zero if there are no transactions.
    let mut next_tx_num = tx_cursor.last()?.map(|(id, _)| id + 1).unwrap_or_default();
    let block_number = block.number;
    let tx_count = block.body.len() as u64;

    // write transaction block index
    if !block.body.is_empty() {
        tx_block_cursor.append(next_tx_num - 1, block_number)?;
    }

    // Write transactions
    for transaction in block.body {
        // Append the transaction
        tx_cursor.append(next_tx_num, transaction.into())?;
        // Increment transaction id for each transaction.
        next_tx_num += 1;
    }

    // Write ommers if any
    if !block.ommers.is_empty() {
        ommers_cursor.append(
            block_number,
            StoredBlockOmmers {
                ommers: block.ommers,
            },
        )?;
    }

    // Write withdrawals if any
    if let Some(withdrawals) = block.withdrawals {
        if !withdrawals.is_empty() {
            withdrawals_cursor.append(block_number, StoredBlockWithdrawals { withdrawals })?;
        }
    }

    // insert block meta
    block_indices_cursor.append(
        block_number,
        StoredBlockBodyIndices {
            first_tx_num: next_tx_num,
            tx_count,
        },
    )?;

    Ok(())
}
