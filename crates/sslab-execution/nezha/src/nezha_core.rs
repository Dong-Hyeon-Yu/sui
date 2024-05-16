use itertools::Itertools;
use rayon::prelude::*;
use reth::{
    primitives::{Block, BlockWithSenders, ChainSpec, Header, Receipt},
    revm::{database::StateProviderDatabase, DatabaseCommit},
};
use sslab_execution::{
    db::{SharableState, SharableStateDB, ThreadSafeCacheState},
    revm_utiles::{init_evm, transact},
    traits::Executable,
    BlockExecutionError, ProviderFactoryMDBX,
};
use std::sync::Arc;

use crate::{
    address_based_conflict_graph::{FastHashMap, KdgKey},
    types::{IndexedEthereumTransaction, ScheduledTransaction, SimulatedTransactionV2},
    KeyBasedDependencyGraph,
};

use super::address_based_conflict_graph::Transaction;

pub struct OptME {
    state: SharableStateDB,
    cached_state: ThreadSafeCacheState,
    client: ProviderFactoryMDBX,
    chain_spec: Arc<ChainSpec>,
    cumulative_gas_used: u64,
}

#[async_trait::async_trait(?Send)]
impl Executable for OptME {
    async fn execute(
        &mut self,
        consensus_output: BlockWithSenders,
    ) -> Result<(Vec<Receipt>, u64), BlockExecutionError> {
        Ok((
            self._execute_inner(consensus_output).await?,
            self.cumulative_gas_used,
        ))
    }

    fn new_with_db(
        db: ProviderFactoryMDBX,
        cached_state: Option<ThreadSafeCacheState>,
        chain_spec: Arc<ChainSpec>,
    ) -> Self {
        let cached_state = cached_state.unwrap_or_default();
        let state = SharableState::builder()
            .with_database(StateProviderDatabase::new(db.latest().unwrap()))
            .with_cached_prestate(cached_state.clone())
            .with_bundle_update()
            .build();

        Self {
            state,
            cached_state,
            client: db,
            chain_spec,
            cumulative_gas_used: 0,
        }
    }
}

impl OptME {
    pub async fn _execute_inner(
        &mut self,
        consensus_output: BlockWithSenders,
    ) -> Result<Vec<Receipt>, BlockExecutionError> {
        self.cumulative_gas_used = 0;

        if consensus_output.block.body.is_empty() {
            return Ok(vec![]);
        }

        let (block, senders) = consensus_output.into_components();
        let Block { header, body, .. } = block;

        let tx_list = body
            .into_iter()
            .zip(senders)
            .enumerate()
            .map(|(id, (tx, signer))| IndexedEthereumTransaction {
                tx,
                signer,
                id: id as u64,
            })
            .collect_vec();

        let mut receipts = vec![];
        let scheduled_aborted_txs: Vec<Vec<IndexedEthereumTransaction>>;

        // 1st execution
        {
            let rw_set = self._simulate(&header, tx_list).await?;

            let ScheduledInfo {
                scheduled_txs,
                aborted_txs,
            } = KeyBasedDependencyGraph::par_construct(rw_set)
                .await
                .hierarchcial_sort()
                .reorder()
                .par_extract_schedule()
                .await;

            // create Receipts of scheduled_txs
            receipts.extend(_make_receipts(
                scheduled_txs.iter().flatten().collect_vec(),
                &mut self.cumulative_gas_used,
            ));

            // reflect state changes into cached state (not dist at this point)
            self._concurrent_commit(scheduled_txs).await;

            scheduled_aborted_txs = aborted_txs;
        }

        for tx_list_to_re_execute in scheduled_aborted_txs.into_iter() {
            // 2nd execution
            //  (1) re-simulation  ----------------> (rw-sets are changed ??)  -------yes-------> (2') invalidate (or, fallback)
            //                                                 |
            //                                                no
            //                                                 |
            //                                          (2) commit

            let rw_sets = self._simulate(&header, tx_list_to_re_execute).await?;

            match self._validate_optimistic_assumption(rw_sets).await {
                None => {}
                Some(invalid_txs) => {
                    //* invalidate */
                    tracing::debug!("invalidated txs: {:?}", invalid_txs);

                    //* fallback */
                    // let ScheduledInfo {scheduled_txs, aborted_txs } = AddressBasedConflictGraph::par_construct(rw_sets).await
                    //     .hierarchcial_sort()
                    //     .reorder()
                    //     .par_extract_schedule().await;

                    // self._concurrent_commit(scheduled_txs).await;

                    //* 3rd execution (serial) for complex transactions */
                    // let snapshot = self.global_state.clone();
                    // tokio::task::spawn_blocking(move || {
                    //     aborted_txs.into_iter()
                    //         .flatten()
                    //         .for_each(|tx| {
                    //             match evm_utils::simulate_tx(tx.raw_tx(), snapshot.as_ref()) {
                    //                 Ok(Some((effect, _, _))) => {
                    //                     snapshot.apply_local_effect(effect);
                    //                 },
                    //                 _ => {
                    //                     warn!("fail to execute a transaction {}", tx.id());
                    //                 }
                    //             }
                    //         });
                    // }).await.expect("fail to spawn a task for serial execution of aborted txs");
                }
            }
        }

        Ok(receipts)
    }

    pub(crate) async fn _simulate(
        &self,
        header: &Header,
        tx_list: Vec<IndexedEthereumTransaction>,
    ) -> Result<Vec<SimulatedTransactionV2>, BlockExecutionError> {
        // Parallel simulation requires heavy cpu usages.
        // CPU-bound jobs would make the I/O-bound tokio threads starve.
        // To this end, a separated thread pool need to be used for cpu-bound jobs.
        // a new thread is created, and a new thread pool is created on the thread. (specifically, rayon's thread pool is created)
        let (send, recv) = tokio::sync::oneshot::channel();
        let client = self.client.clone();
        let chain_spec = self.chain_spec.clone();
        let cache = self.cached_state.clone();
        let header = header.clone();

        rayon::spawn(move || {
            let result = tx_list
                .into_par_iter()
                .map(|tx| {
                    let db = SharableState::builder()
                        .with_database_boxed(Box::new(StateProviderDatabase::new(
                            client.latest().unwrap(),
                        )))
                        .with_cached_prestate(cache.clone())
                        .with_bundle_update()
                        .build();

                    // configure evm with the given header and chain spec.
                    let mut evm = init_evm(&header.clone(), &chain_spec.clone(), db)?;

                    let result = transact(&mut evm, tx.data(), tx.signer())?;

                    Ok(SimulatedTransactionV2::new(
                        result,
                        evm.extract_rw_set(),
                        tx,
                        evm.context.evm.db.take_bundle(),
                    ))
                })
                .collect();

            let _ = send.send(result).unwrap();
        });

        match recv.await {
            Ok(rw_sets) => rw_sets,
            Err(e) => {
                panic!(
                    "fail to receive simulation result from the worker thread. {:?}",
                    e
                );
            }
        }
    }

    pub async fn _concurrent_commit(&self, scheduled_txs: Vec<Vec<ScheduledTransaction>>) {
        let storage = self.cached_state.clone();

        // Parallel simulation requires heavy cpu usages.
        // CPU-bound jobs would make the I/O-bound tokio threads starve.
        // To this end, a separated thread pool need to be used for cpu-bound jobs.
        // a new thread is created, and a new thread pool is created on the thread. (specifically, rayon's thread pool is created)
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let _storage = &storage;
            for txs_to_commit in scheduled_txs {
                txs_to_commit.into_par_iter().for_each(|tx| {
                    let effect = tx.extract();
                    _storage.apply_evm_state(effect);
                })
            }
            let _ = send.send(());
        });

        let _ = recv.await;
    }

    async fn _validate_optimistic_assumption(
        &self,
        rw_set: Vec<SimulatedTransactionV2>,
    ) -> Option<Vec<SimulatedTransactionV2>> {
        if rw_set.len() == 1 {
            let SimulatedTransactionV2 { effects, .. } = rw_set[0].clone();

            self.state.commit(effects);

            return None;
        }

        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let mut valid_txs = vec![];
            let mut invalid_txs = vec![];

            let mut write_set = hashbrown::HashSet::<KdgKey>::new();
            for tx in rw_set.into_iter() {
                match tx.write_set() {
                    Some(set) => {
                        if (set.len() <= write_set.len() && set.is_disjoint(&write_set)) // select smaller set using short-circuit evaluation
                            || (set.len() > write_set.len() && write_set.is_disjoint(&set))
                        {
                            write_set.extend(set);
                            valid_txs.push(tx);
                        } else {
                            invalid_txs.push(tx);
                        }
                    }
                    None => {
                        valid_txs.push(tx);
                    }
                }
            }

            if invalid_txs.is_empty() {
                let _ = send.send((valid_txs, None));
            } else {
                let _ = send.send((valid_txs, Some(invalid_txs)));
            }
        });

        let (valid_txs, invalid_txs) = recv.await.unwrap();

        self._concurrent_commit_2(valid_txs).await;
        //TODO: create Receipts of valid_txs

        invalid_txs
    }

    #[inline]
    pub async fn _concurrent_commit_2(&self, scheduled_txs: Vec<SimulatedTransactionV2>) {
        let scheduled_txs = vec![_convert(scheduled_txs).await];

        let storage = self.cached_state.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let _storage = &storage;
            for txs_to_commit in scheduled_txs {
                txs_to_commit.into_iter().for_each(|tx| {
                    let effect = tx.extract();
                    _storage.apply_evm_state(effect);
                })
            }
            let _ = send.send(());
        });

        let _ = recv.await;
    }
}

// #[cfg(feature = "latency")]
use tokio::time::Instant;

// #[cfg(feature = "latency")]
#[async_trait::async_trait]
pub trait LatencyBenchmark {
    async fn _execute_and_return_latency(
        &mut self,
        consensus_output: BlockWithSenders,
    ) -> Result<(u128, u128, u128, (u128, u128), u128), BlockExecutionError>;

    async fn _validate_optimistic_assumption_and_return_latency(
        &self,
        rw_set: Vec<SimulatedTransactionV2>,
    ) -> (Option<Vec<SimulatedTransactionV2>>, u128, u128);
}

// #[cfg(feature = "latency")]
#[async_trait::async_trait]
impl LatencyBenchmark for OptME {
    async fn _execute_and_return_latency(
        &mut self,
        consensus_output: BlockWithSenders,
    ) -> Result<(u128, u128, u128, (u128, u128), u128), BlockExecutionError> {
        self.cumulative_gas_used = 0;

        if consensus_output.block.body.is_empty() {
            return Ok((0, 0, 0, (0, 0), 0));
        }

        let (block, senders) = consensus_output.into_components();
        let Block { header, body, .. } = block;

        let tx_list = body
            .into_iter()
            .zip(senders)
            .enumerate()
            .map(|(id, (tx, signer))| IndexedEthereumTransaction {
                tx,
                signer,
                id: id as u64,
            })
            .collect_vec();

        // let mut receipts = vec![];
        let scheduled_aborted_txs: Vec<Vec<IndexedEthereumTransaction>>;

        let mut simulation_latency = 0;
        let mut scheduling_latency = 0;
        let mut validation_latency = 0;
        let mut validation_execute_latency = 0;
        let mut commit_latency = 0;

        let total_latency = Instant::now();
        // 1st execution
        {
            let latency = Instant::now();
            let rw_sets = self._simulate(&header, tx_list).await?;
            simulation_latency += latency.elapsed().as_micros();

            let latency = Instant::now();
            let ScheduledInfo {
                scheduled_txs,
                aborted_txs,
            } = KeyBasedDependencyGraph::par_construct(rw_sets)
                .await
                .hierarchcial_sort()
                .reorder()
                .par_extract_schedule()
                .await;
            scheduling_latency += latency.elapsed().as_micros();

            let latency = Instant::now();
            self._concurrent_commit(scheduled_txs).await;
            commit_latency += latency.elapsed().as_micros();

            scheduled_aborted_txs = aborted_txs;
        }

        for tx_list_to_re_execute in scheduled_aborted_txs.into_iter() {
            // 2nd execution
            //  (1) re-simulation  ----------------> (rw-sets are changed ??)  -------yes-------> (2') invalidate (or, fallback)
            //                                                 |
            //                                                no
            //                                                 |
            //                                          (2) commit

            let latency = Instant::now();
            let rw_sets = self._simulate(&header, tx_list_to_re_execute).await?;
            validation_execute_latency += latency.elapsed().as_micros();

            match self
                ._validate_optimistic_assumption_and_return_latency(rw_sets)
                .await
            {
                (None, v, c) => {
                    commit_latency += c;
                    validation_latency += v;
                }
                (Some(invalid_txs), v, c) => {
                    commit_latency += c;
                    validation_latency += v;

                    //* invalidate */
                    tracing::debug!("invalidated txs: {:?}", invalid_txs);
                }
            }
        }

        Ok((
            total_latency.elapsed().as_micros(),
            simulation_latency,
            scheduling_latency,
            (validation_latency, validation_execute_latency),
            commit_latency,
        ))
    }

    async fn _validate_optimistic_assumption_and_return_latency(
        &self,
        rw_set: Vec<SimulatedTransactionV2>,
    ) -> (Option<Vec<SimulatedTransactionV2>>, u128, u128) {
        if rw_set.len() == 1 {
            let SimulatedTransactionV2 { effects, .. } = rw_set[0].clone();

            let latency = Instant::now();
            self.cached_state.apply_evm_state(effects);
            let c_latency = latency.elapsed().as_micros();

            return (None, 0, c_latency);
        }

        let (send, recv) = tokio::sync::oneshot::channel();

        let latency = Instant::now();
        rayon::spawn(move || {
            let mut valid_txs = vec![];
            let mut invalid_txs = vec![];

            let mut write_set = hashbrown::HashSet::<KdgKey>::new();
            for tx in rw_set.into_iter() {
                match tx.write_set() {
                    Some(ref set) => {
                        if (set.len() <= write_set.len() && set.is_disjoint(&write_set)) // select smaller set using short-circuit evaluation
                            || (set.len() > write_set.len() && write_set.is_disjoint(set))
                        {
                            write_set.extend(set);
                            valid_txs.push(tx);
                        } else {
                            invalid_txs.push(tx);
                        }
                    }
                    None => {
                        valid_txs.push(tx);
                    }
                }
            }

            if invalid_txs.is_empty() {
                let _ = send.send((valid_txs, None));
            } else {
                let _ = send.send((valid_txs, Some(invalid_txs)));
            }
        });

        let (valid_txs, invalid_txs) = recv.await.unwrap();
        let validation_latency = latency.elapsed().as_micros();

        let scheduled_txs = vec![_convert(valid_txs).await];

        let commit_latency = Instant::now();
        let storage = self.cached_state.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let _storage = &storage;
            for txs_to_commit in scheduled_txs {
                txs_to_commit.into_iter().for_each(|tx| {
                    let effect = tx.extract();
                    _storage.apply_evm_state(effect);
                })
            }
            let _ = send.send(());
        });

        let _ = recv.await;
        let c_latency = commit_latency.elapsed().as_micros();
        (invalid_txs, validation_latency, c_latency)
    }
}

pub struct ScheduledInfo {
    pub scheduled_txs: Vec<Vec<ScheduledTransaction>>,
    pub aborted_txs: Vec<Vec<IndexedEthereumTransaction>>,
}

impl ScheduledInfo {
    pub fn from(
        tx_list: FastHashMap<u64, Arc<Transaction>>,
        aborted_txs: Vec<Arc<Transaction>>,
    ) -> Self {
        let aborted_txs = Self::_schedule_aborted_txs(aborted_txs, false);
        let scheduled_txs = Self::_schedule_sorted_txs(tx_list, false);

        Self {
            scheduled_txs,
            aborted_txs,
        }
    }

    pub fn par_from(
        tx_list: FastHashMap<u64, Arc<Transaction>>,
        aborted_txs: Vec<Arc<Transaction>>,
    ) -> Self {
        let aborted_txs = Self::_schedule_aborted_txs(aborted_txs, true);
        let scheduled_txs = Self::_schedule_sorted_txs(tx_list, true);

        Self {
            scheduled_txs,
            aborted_txs,
        }
    }

    fn _schedule_sorted_txs(
        tx_list: FastHashMap<u64, Arc<Transaction>>,
        rayon: bool,
    ) -> Vec<Vec<ScheduledTransaction>> {
        let mut list = if rayon {
            tx_list
                .par_iter()
                .for_each(|(_, tx)| tx.clear_write_units());

            tx_list
                .into_par_iter()
                .map(|(_, tx)| ScheduledTransaction::from(_unwrap(tx)))
                .collect::<Vec<ScheduledTransaction>>()
        } else {
            tx_list.iter().for_each(|(_, tx)| tx.clear_write_units());

            tx_list
                .into_iter()
                .map(|(_, tx)| ScheduledTransaction::from(_unwrap(tx)))
                .collect::<Vec<ScheduledTransaction>>()
        };

        // sort groups by sequence.
        list.sort_by_key(|tx| tx.seq());
        let mut scheduled_txs = Vec::<Vec<ScheduledTransaction>>::new();
        for (_key, txns) in &list.into_iter().group_by(|tx| tx.seq()) {
            scheduled_txs.push(txns.sorted_unstable_by_key(|tx| tx.id()).collect_vec());
        }

        scheduled_txs
    }

    fn _schedule_aborted_txs(
        mut aborted_txs: Vec<Arc<Transaction>>,
        rayon: bool,
    ) -> Vec<Vec<IndexedEthereumTransaction>> {
        if rayon {
            aborted_txs.par_iter().for_each(|tx| {
                tx.clear_write_units();
                tx.init();
            });
        } else {
            aborted_txs.iter().for_each(|tx| {
                tx.clear_write_units();
                tx.init();
            });
        };

        // determine minimum #epoch in which tx have no conflicts with others --> by binary-search over a map (#epoch, writeset)
        let mut epoch_map: Vec<hashbrown::HashSet<KdgKey>> = vec![]; // (epoch, write set)

        // store final schedule information
        let mut schedule: Vec<Vec<IndexedEthereumTransaction>> = vec![];

        aborted_txs.sort_unstable_by_key(|tx| tx.id());

        for tx in aborted_txs.into_iter().map(_unwrap) {
            let (read_keys, write_keys) = tx.prev_rw_set();

            let epoch =
                Self::_find_minimun_epoch_with_no_conflicts(&read_keys, &write_keys, &epoch_map);

            // update epoch_map & schedule
            match epoch_map.get_mut(epoch) {
                Some(w_map) => {
                    w_map.extend(write_keys);
                    schedule[epoch].push(tx.raw_tx());
                }
                None => {
                    epoch_map.push(write_keys);
                    schedule.push(vec![tx.raw_tx()]);
                }
            };
        }

        schedule
    }

    fn _find_minimun_epoch_with_no_conflicts(
        read_keys_of_tx: &hashbrown::HashSet<KdgKey>,
        write_keys_of_tx: &hashbrown::HashSet<KdgKey>,
        epoch_map: &Vec<hashbrown::HashSet<KdgKey>>,
    ) -> usize {
        // 1) ww dependencies are occured when the keys which are both read and written by latter tx are overlapped with the rw keys of the previous txs in the same epoch.
        //   for simplicity, only single write is allowed for each key in the same epoch.

        // 2) anti-rw dependencies are occured when the read keys of latter tx are overlapped with the write keys of the previous txs in the same epoch.
        let keys_of_tx = read_keys_of_tx
            .union(write_keys_of_tx)
            .cloned()
            .collect::<hashbrown::HashSet<_>>();

        let mut epoch = 0;
        while epoch_map.len() > epoch && !keys_of_tx.is_disjoint(&epoch_map[epoch]) {
            epoch += 1;
        }

        epoch
    }

    pub fn scheduled_txs_len(&self) -> usize {
        self.scheduled_txs.iter().map(|vec| vec.len()).sum()
    }

    pub fn aborted_txs_len(&self) -> usize {
        self.aborted_txs.iter().map(|vec| vec.len()).sum()
    }

    pub fn parallism_metric(&self) -> (usize, f64, f64, usize, usize) {
        let total_tx = self.scheduled_txs_len() + self.aborted_txs_len();
        let max_width = self
            .scheduled_txs
            .iter()
            .map(|vec| vec.len())
            .max()
            .unwrap_or(0);
        let depth = self.scheduled_txs.len();
        let average_width = self
            .scheduled_txs
            .iter()
            .map(|vec| vec.len())
            .sum::<usize>() as f64
            / depth as f64;
        let var_width = self
            .scheduled_txs
            .iter()
            .map(|vec| vec.len())
            .fold(0.0, |acc, len| acc + (len as f64 - average_width).powi(2))
            / depth as f64;
        let std_width = var_width.sqrt();
        (total_tx, average_width, std_width, max_width, depth)
    }
}

#[inline]
fn _unwrap(tx: Arc<Transaction>) -> Transaction {
    match Arc::try_unwrap(tx) {
        Ok(tx) => tx,
        Err(tx) => {
            panic!(
                "fail to unwrap transaction. (strong:{}, weak:{}): {:?}",
                Arc::strong_count(&tx),
                Arc::weak_count(&tx),
                tx
            );
        }
    }
}

#[inline]
async fn _convert(txs: Vec<SimulatedTransactionV2>) -> Vec<ScheduledTransaction> {
    let (send, recv) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        let result = txs
            .into_iter()
            .map(ScheduledTransaction::from)
            .collect_vec();

        let _ = send.send(result).unwrap();
    });
    recv.await.unwrap()
}

fn _make_receipts(txs: Vec<&ScheduledTransaction>, cumulative_gas_used: &mut u64) -> Vec<Receipt> {
    let receipts = txs
        .into_iter()
        .map(|tx| {
            *cumulative_gas_used += tx.result.gas_used();

            Receipt {
                tx_type: tx.tx_type,
                // Success flag was added in `EIP-658: Embedding transaction status code in
                // receipts`.
                success: tx.result.is_success(),
                cumulative_gas_used: *cumulative_gas_used,
                // convert to reth log
                logs: tx.result.logs().into_iter().map(Into::into).collect(),
            }
        })
        .collect_vec();

    receipts
}
