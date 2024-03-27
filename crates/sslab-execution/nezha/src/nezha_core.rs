use ethers_core::types::H256;
use hashbrown::HashSet;
use incr_stats::incr::Stats;
use itertools::Itertools;
use narwhal_types::BatchDigest;
use rayon::prelude::*;
use sslab_execution::{
    evm_storage::{backend::ExecutionBackend, ConcurrentEVMStorage},
    executor::Executable,
    types::{ExecutableEthereumBatch, ExecutionResult, IndexedEthereumTransaction},
};
use std::sync::Arc;
use tracing::warn;

use crate::{
    address_based_conflict_graph::FastHashMap, types::ScheduledTransaction,
    AddressBasedConflictGraph, SimulationResult,
};

use super::{address_based_conflict_graph::Transaction, types::SimulatedTransaction};

#[async_trait::async_trait]
impl Executable for Nezha {
    async fn execute(&self, consensus_output: Vec<ExecutableEthereumBatch>) {
        let _ = self.inner.prepare_execution(consensus_output).await;
    }
}

pub struct Nezha {
    inner: ConcurrencyLevelManager,
}

impl Nezha {
    pub fn new(global_state: ConcurrentEVMStorage, concurrency_level: usize) -> Self {
        Self {
            inner: ConcurrencyLevelManager::new(global_state, concurrency_level),
        }
    }
}

pub struct ConcurrencyLevelManager {
    concurrency_level: usize,
    global_state: Arc<ConcurrentEVMStorage>,
}

impl ConcurrencyLevelManager {
    pub fn new(global_state: ConcurrentEVMStorage, concurrency_level: usize) -> Self {
        Self {
            global_state: Arc::new(global_state),
            concurrency_level,
        }
    }

    async fn prepare_execution(
        &self,
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> ExecutionResult {
        let mut result = vec![];
        let mut target = consensus_output;

        while !target.is_empty() {
            let split_idx = std::cmp::min(self.concurrency_level, target.len());
            let remains: Vec<ExecutableEthereumBatch> = target.split_off(split_idx);

            result.extend(self._execute(target).await);

            target = remains;
        }

        ExecutionResult::new(result)
    }

    async fn _unpack_batches(
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> (Vec<BatchDigest>, Vec<IndexedEthereumTransaction>) {
        let (send, recv) = tokio::sync::oneshot::channel();

        rayon::spawn(move || {
            let (digests, batches): (Vec<_>, Vec<_>) = consensus_output
                .par_iter()
                .map(|batch| (batch.digest().to_owned(), batch.data().to_owned()))
                .unzip();

            let tx_list = batches
                .into_iter()
                .flatten()
                .enumerate()
                .map(|(id, tx)| IndexedEthereumTransaction::new(tx, id as u64))
                .collect::<Vec<_>>();

            let _ = send.send((digests, tx_list)).unwrap();
        });

        recv.await.unwrap()
    }

    // #[cfg(feature = "test")]
    pub async fn _execute_and_return_parellism_metric(
        &self,
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> (f64, f64, f64, f64, f64, u32) {
        let mut stat = Stats::new();

        let (_, mut tx_list) = Self::_unpack_batches(consensus_output).await;

        while tx_list.len() > 0 {
            let rw_sets = self._simulate(tx_list).await;

            let ScheduledInfo {
                scheduled_txs,
                aborted_txs,
            } = AddressBasedConflictGraph::par_construct(rw_sets)
                .await
                .hierarchcial_sort()
                .reorder()
                .par_extract_schedule()
                .await;

            scheduled_txs.iter().for_each(|seq| {
                stat.update(seq.len() as f64).ok();
            });

            tx_list = aborted_txs;
        }

        let metric = (
            stat.sum().unwrap_or_default(),
            stat.mean().unwrap_or_default(),
            stat.population_standard_deviation().unwrap_or_default(),
            stat.population_skewness().unwrap_or_default(),
            stat.max().unwrap_or_default(),
            stat.count(),
        );

        metric
    }

    pub async fn _execute(
        &self,
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> Vec<BatchDigest> {
        let (digests, mut tx_list) = Self::_unpack_batches(consensus_output).await;

        while tx_list.len() > 0 {
            let rw_sets = self._simulate(tx_list).await;

            let ScheduledInfo {
                scheduled_txs,
                aborted_txs,
            } = AddressBasedConflictGraph::par_construct(rw_sets)
                .await
                .hierarchcial_sort()
                .reorder()
                .par_extract_schedule()
                .await;

            self._concurrent_commit(scheduled_txs).await;

            tx_list = aborted_txs;
        }

        digests
    }

    pub async fn simulate(
        &self,
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> SimulationResult {
        let (digests, tx_list) = Self::_unpack_batches(consensus_output).await;
        let rw_sets = self._simulate(tx_list).await;

        SimulationResult { digests, rw_sets }
    }

    async fn _simulate(
        &self,
        tx_list: Vec<IndexedEthereumTransaction>,
    ) -> Vec<SimulatedTransaction> {
        let snapshot = self.global_state.clone();

        // Parallel simulation requires heavy cpu usages.
        // CPU-bound jobs would make the I/O-bound tokio threads starve.
        // To this end, a separated thread pool need to be used for cpu-bound jobs.
        // a new thread is created, and a new thread pool is created on the thread. (specifically, rayon's thread pool is created)
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let result = tx_list
                .into_par_iter()
                .filter_map(|tx| {
                    match crate::evm_utils::simulate_tx(tx.data(), snapshot.as_ref()) {
                        Ok(Some((effect, log, rw_set))) => {
                            Some(SimulatedTransaction::new(Some(rw_set), effect, log, tx))
                        }
                        _ => {
                            warn!("fail to execute a transaction {}", tx.digest_u64());
                            None
                        }
                    }
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

    //TODO: (optimization) commit the last write of each key
    pub async fn _concurrent_commit(&self, scheduled_txs: Vec<Vec<ScheduledTransaction>>) {
        let storage = self.global_state.clone();

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
                    _storage.apply_local_effect(effect)
                })
            }
            let _ = send.send(());
        });

        let _ = recv.await;
    }
}

pub struct ScheduledInfo {
    pub scheduled_txs: Vec<Vec<ScheduledTransaction>>,
    pub aborted_txs: Vec<IndexedEthereumTransaction>,
}

impl ScheduledInfo {
    pub fn from(
        tx_list: FastHashMap<u64, Arc<Transaction>>,
        aborted_txs: Vec<Arc<Transaction>>,
    ) -> Self {
        let aborted_txs = Self::_process_aborted_txs(aborted_txs, false);
        let scheduled_txs = Self::_schedule_for_sorted_txs(tx_list, false);

        Self {
            scheduled_txs,
            aborted_txs,
        }
    }

    pub fn par_from(
        tx_list: FastHashMap<u64, Arc<Transaction>>,
        aborted_txs: Vec<Arc<Transaction>>,
    ) -> Self {
        let aborted_txs = Self::_process_aborted_txs(aborted_txs, true);
        let scheduled_txs = Self::_schedule_for_sorted_txs(tx_list, true);

        Self {
            scheduled_txs,
            aborted_txs,
        }
    }

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

    fn _schedule_for_sorted_txs(
        tx_list: FastHashMap<u64, Arc<Transaction>>,
        rayon: bool,
    ) -> Vec<Vec<ScheduledTransaction>> {
        let mut list = if rayon {
            tx_list
                .par_iter()
                .for_each(|(_, tx)| tx.clear_write_units());

            tx_list
                .into_par_iter()
                .map(|(_, tx)| {
                    let tx = Self::_unwrap(tx); // TODO: memory leak (let tx = Self::_unwrap(tx);)

                    ScheduledTransaction::from(tx)
                })
                .collect::<Vec<ScheduledTransaction>>()
        } else {
            tx_list.iter().for_each(|(_, tx)| tx.clear_write_units());

            tx_list
                .into_iter()
                .map(|(_, tx)| {
                    let tx = Self::_unwrap(tx); // TODO: memory leak (let tx = Self::_unwrap(tx);)

                    ScheduledTransaction::from(tx)
                })
                .collect::<Vec<ScheduledTransaction>>()
        };

        // sort groups by sequence.
        list.sort_by_key(|tx| tx.seq());
        let mut scheduled_txs = Vec::<Vec<ScheduledTransaction>>::new();
        for (_key, txns) in &list.into_iter().group_by(|tx| tx.seq()) {
            scheduled_txs.push(txns.collect_vec());
        }

        scheduled_txs
    }

    fn _process_aborted_txs(
        aborted_txs: Vec<Arc<Transaction>>,
        rayon: bool,
    ) -> Vec<IndexedEthereumTransaction> {
        if rayon {
            aborted_txs.par_iter().for_each(|tx| {
                tx.clear_write_units();
                tx.init();
            });

            let mut ret = aborted_txs
                .into_par_iter()
                .map(|tx| tx.raw_tx().to_owned())
                .collect::<Vec<_>>();

            ret.sort_by_key(|tx| tx.id);

            ret
        } else {
            aborted_txs.iter().for_each(|tx| {
                tx.clear_write_units();
                tx.init();
            });

            let mut ret = aborted_txs
                .into_iter()
                .map(|tx| tx.raw_tx().to_owned())
                .collect::<Vec<_>>();

            ret.sort_by_key(|tx| tx.id);

            ret
        }
    }

    // anti-rw dependencies are occured when the read keys of latter tx are overlapped with the write keys of the previous txs in the same epoch.
    fn _check_anti_rw_dependencies(
        read_keys_of_tx: &hashbrown::HashSet<H256>, // read keys of an aborted tx
        write_keys_in_specific_epoch: &hashbrown::HashSet<H256>, // a map (epoch, write_keys_set) to prevent anti-rw dependencies
    ) -> bool {
        write_keys_in_specific_epoch.is_disjoint(read_keys_of_tx)
    }

    // TODO: allow only one write in one key.
    // ww dependencies are occured when the keys which are both read and written by latter tx are overlapped with the rw keys of the previous txs in the same epoch.
    fn _check_ww_dependencies(
        write_keys_of_tx: &hashbrown::HashSet<H256>, // keys where tx has both read & write
        write_keys_in_specific_epoch: &hashbrown::HashSet<H256>, // a map (epoch, keys which have both read and write of tx) to prevent ww dependencies
    ) -> bool {
        write_keys_in_specific_epoch.is_disjoint(write_keys_of_tx)
    }

    fn _find_minimun_epoch_with_no_conflicts(
        read_keys_of_tx: &HashSet<H256>,
        write_keys_of_tx: &HashSet<H256>,
        w_map: &HashSet<H256>,
    ) -> std::cmp::Ordering {
        match Self::_check_anti_rw_dependencies(read_keys_of_tx, w_map)
            && Self::_check_ww_dependencies(write_keys_of_tx, w_map)
        {
            true => std::cmp::Ordering::Greater,
            false => std::cmp::Ordering::Less,
        }
    }

    pub fn scheduled_txs_len(&self) -> usize {
        self.scheduled_txs.iter().map(|vec| vec.len()).sum()
    }

    pub fn aborted_txs_len(&self) -> usize {
        self.aborted_txs.len()
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
