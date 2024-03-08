use std::sync::Arc;
use itertools::Itertools;
use narwhal_types::BatchDigest;
use rayon::prelude::*;
use rayon::iter::Either;
use sslab_execution::{
    types::{ExecutableEthereumBatch, ExecutionResult, IndexedEthereumTransaction}, 
    executor::Executable, 
    evm_storage::{ConcurrentEVMStorage, backend::ExecutionBackend}
};
use tracing::warn;

use crate::{address_based_conflict_graph::FastHashMap, types::ScheduledTransaction, AddressBasedConflictGraph, SimulationResult};

use super::{
    types::SimulatedTransaction, 
    address_based_conflict_graph::Transaction,
};

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
    pub fn new(
        global_state: ConcurrentEVMStorage, 
        concurrency_level: usize
    ) -> Self {
        Self {
            inner: ConcurrencyLevelManager::new(global_state, concurrency_level),
        }
    }
}

pub struct ConcurrencyLevelManager {
    concurrency_level: usize,
    global_state: Arc<ConcurrentEVMStorage>
}

impl ConcurrencyLevelManager {
    
    pub fn new(global_state: ConcurrentEVMStorage, concurrency_level: usize) -> Self {
        Self {
            global_state: Arc::new(global_state),
            concurrency_level
        }
    }

    async fn prepare_execution(&self, consensus_output: Vec<ExecutableEthereumBatch>) -> ExecutionResult {

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

    async fn _unpack_batches(consensus_output: Vec<ExecutableEthereumBatch>) -> (Vec<BatchDigest>, Vec<IndexedEthereumTransaction>){

        let (send, recv) = tokio::sync::oneshot::channel();

        rayon::spawn(move || {

            let (digests, batches): (Vec<_>, Vec<_>) = consensus_output
                .par_iter()
                .map(|batch| {

                    (batch.digest().to_owned(), batch.data().to_owned())
                })
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

    pub async fn _execute(&self, consensus_output: Vec<ExecutableEthereumBatch>) -> Vec<BatchDigest> {
        
        let (digests, tx_list) = Self::_unpack_batches(consensus_output).await;

        let mut remains;

        // 1st execution
        {
            let rw_sets = self._simulate(tx_list).await;
        
            let ScheduledInfo {scheduled_txs, aborted_txs } = AddressBasedConflictGraph::par_construct(rw_sets).await
                .hierarchcial_sort()
                .reorder()
                .par_extract_schedule().await;

            self._concurrent_commit(scheduled_txs).await;

            remains = aborted_txs;
        }

        let mut epoch = 1u32;
        while !remains.is_empty() {
            // optimistic scheduling
            let ScheduledInfo {scheduled_txs, aborted_txs } = AddressBasedConflictGraph::par_optimistic_construct(remains).await
                .hierarchcial_sort()
                .reorder()
                .par_extract_schedule().await;

            if scheduled_txs.is_empty() {
                println!("(epoch # {}) aborted transactions({}): {:?}", epoch, aborted_txs.len(), aborted_txs);
                panic!("endless loop!");
            }

            // validation by speculative execution
            for txs in scheduled_txs {
                let raw_tx_list = txs.par_iter().map(|tx| tx.raw_tx()).cloned().collect();
                let simulated_txs = self._simulate(raw_tx_list).await;
                
                match self._validate_optimistic_assumption(txs, simulated_txs).await { 
                    Some(invalid_txs) => {

                        // invalidate txs
                        tracing::debug!("(epoch # {}) invalidate txs: {:?}", epoch, invalid_txs.len());

                        /* fallback to serial execution */
                        // let snapshot = self.global_state.clone();
                        // tokio::task::spawn_blocking(move || {
                        //     simulated_txs.into_iter()
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
                    },
                    None => {}
                }
            }

            remains = aborted_txs;
            epoch += 1;
        }
        

        digests
    }

    pub async fn simulate(&self, consensus_output: Vec<ExecutableEthereumBatch>) -> SimulationResult {
        let (digests, tx_list) = Self::_unpack_batches(consensus_output).await;
        let rw_sets = self._simulate(tx_list).await;

        SimulationResult {digests, rw_sets}
    }
    

    async fn _simulate(&self, tx_list: Vec<IndexedEthereumTransaction>) -> Vec<SimulatedTransaction> {
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
                        },
                        _ => {
                            warn!("fail to execute a transaction {}", tx.digest_u64());
                            None
                        },
                    }
                })
                .collect();

                let _ = send.send(result).unwrap();
        });

        match recv.await {
            Ok(rw_sets) => {
                rw_sets 
            },
            Err(e) => {
                panic!("fail to receive simulation result from the worker thread. {:?}", e);
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
                txs_to_commit
                    .into_par_iter()
                    .for_each(|tx| {
                        let effect = tx.extract();
                        _storage.apply_local_effect(effect)
                    })
            }
            let _ = send.send(());
        });

        let _ = recv.await;
    }


    async fn _validate_optimistic_assumption(&self, previous_tx: Vec<ScheduledTransaction>, mut rw_set: Vec<SimulatedTransaction>) -> Option<Vec<SimulatedTransaction>> {

        if rw_set.len() == 1 {
            self._concurrent_commit(vec![vec![ScheduledTransaction::from(rw_set.pop().unwrap())]]).await;
            return None;
        }

        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {

            // validation optimistic assumption: RW keys are not changed after re-execution.
            let (mut valid_list, _invalid_list): (Vec<_>, Vec<_>) = previous_tx
                .iter()
                .zip(rw_set)
                .partition_map(|(prev, cur)| {
                    let (prev_write_set, prev_read_set) = prev.rw_set();
                    if prev_write_set == cur.write_set().unwrap() && prev_read_set == cur.read_set().unwrap() {
                        Either::Left(cur)
                    }
                    else {
                        Either::Right(cur)
                    }
            });

            // allow only one write in one key.
            let mut invalid_list = vec![];
            if _invalid_list.len() > 0 {

                let mut write_set: hashbrown::HashSet<_> = valid_list
                    .iter()
                    .fold(hashbrown::HashSet::new(), 
                        |mut set, tx|  {
                            set.extend(tx.write_set().unwrap());
                            set
                    });

                for tx in _invalid_list.into_iter() {
                    match tx.write_set() {
                        Some(ref set) => {
                            if (set.len() <= write_set.len() && set.is_disjoint(&write_set)) // select smaller set using short-circuit evaluation
                                || (set.len() > write_set.len() && write_set.is_disjoint(set))  {
                                write_set.extend(set);
                                valid_list.push(tx);
                            }
                            else {
                                invalid_list.push(tx);
                            }
                        },
                        None => {
                            valid_list.push(tx);
                        },
                    }
                };
            }

            send.send((valid_list, invalid_list)).unwrap();
        });

        match recv.await {
            Ok((valid_list, invalid_list)) => {
                self._concurrent_commit_2(valid_list).await;
                if invalid_list.len() > 0 {
                    Some(invalid_list)
                }
                else {
                    None
                }
            },
            Err(e) => {
                panic!("fail to receive validation result from the worker thread. {:?}", e);
            }
        }
    }

    pub async fn _concurrent_commit_2(&self, scheduled_txs: Vec<SimulatedTransaction>) {
        let scheduled_txs = vec![
            tokio::task::spawn_blocking(move || {
                scheduled_txs.into_par_iter().map(|tx| ScheduledTransaction::from(tx)).collect()
            }).await.expect("fail to spawn a task for convert SimulatedTransaction to ScheduledTransaction")
        ];
         
        self._concurrent_commit(scheduled_txs).await;
    }    
}



pub struct ScheduledInfo {
    pub scheduled_txs: Vec<Vec<ScheduledTransaction>>,  
    pub aborted_txs: Vec<Arc<Transaction>>,
}

impl ScheduledInfo {

    pub fn from(tx_list: FastHashMap<u64, Arc<Transaction>>, aborted_txs: Vec<Arc<Transaction>>) -> Self {

        let aborted_txs = Self::_process_aborted_txs(aborted_txs, false);
        let scheduled_txs = Self::_schedule_for_sorted_txs(tx_list, false);
        
        Self { scheduled_txs, aborted_txs }
    }


    pub fn par_from(tx_list: FastHashMap<u64, Arc<Transaction>>, aborted_txs: Vec<Arc<Transaction>>) -> Self {

        let aborted_txs = Self::_process_aborted_txs(aborted_txs, true);
        let scheduled_txs = Self::_schedule_for_sorted_txs(tx_list, true);
        
        Self { scheduled_txs, aborted_txs }
    }

    fn _unwrap(tx: Arc<Transaction>) -> Transaction {
        match Arc::try_unwrap(tx) {
            Ok(tx) => tx,
            Err(tx) => {
                panic!("fail to unwrap transaction. (strong:{}, weak:{}): {:?}", Arc::strong_count(&tx), Arc::weak_count(&tx), tx);
            }
        }
    }

    fn _schedule_for_sorted_txs(tx_list: FastHashMap<u64, Arc<Transaction>>, rayon: bool) -> Vec<Vec<ScheduledTransaction>> {
        let mut list = if rayon {
            tx_list.par_iter().for_each(|(_, tx)| tx.clear_write_units());

            tx_list.into_par_iter()
                .map(|(_, tx)| {
                    // TODO: memory leak (let tx = Self::_unwrap(tx);) 
    
                    ScheduledTransaction::from(tx)
                })
                .collect::<Vec<ScheduledTransaction>>() 
        }
        else {
            tx_list.iter().for_each(|(_, tx)| tx.clear_write_units());

            tx_list.into_iter()
                .map(|(_, tx)| {
                    // TODO: memory leak (let tx = Self::_unwrap(tx);)  

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

    
    fn _process_aborted_txs(mut aborted_txs: Vec<Arc<Transaction>>, rayon: bool) -> Vec<Arc<Transaction>> {
        if rayon {
            aborted_txs.par_iter()
                .for_each(|tx| {
                    tx.clear_write_units();
                    tx.init();
                }
            );
        }
        else {
            aborted_txs.iter()
            .for_each(|tx| {
                tx.clear_write_units();
                tx.init();
            });
        };

        aborted_txs.sort_by_key(|tx| tx.id());

        aborted_txs
    }

    pub fn scheduled_txs_len(&self) -> usize {
        self.scheduled_txs.iter().map(|vec| vec.len()).sum()
    }

    pub fn aborted_txs_len(&self) -> usize {
        self.aborted_txs.len()
    }

    pub fn parallism_metric(&self) -> (usize, f64, f64, usize, usize) {
        let total_tx = self.scheduled_txs_len()+self.aborted_txs_len();
        let max_width = self.scheduled_txs.iter().map(|vec| vec.len()).max().unwrap_or(0);
        let depth = self.scheduled_txs.len();
        let average_width = self.scheduled_txs.iter().map(|vec| vec.len()).sum::<usize>() as f64 / depth as f64;
        let var_width = self.scheduled_txs.iter().map(|vec| vec.len()).fold(0.0, |acc, len| acc + (len as f64 - average_width).powi(2)) / depth as f64;
        let std_width = var_width.sqrt();
        (total_tx, average_width, std_width, max_width, depth)
    }
}

