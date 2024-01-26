use std::{sync::Arc, rc::Rc};
use ethers_core::types::H256;
use itertools::Itertools;
use narwhal_types::BatchDigest;
use rayon::prelude::*;
use sslab_execution::{
    types::{ExecutableEthereumBatch, ExecutionResult}, 
    executor::Executable, 
    evm_storage::{ConcurrentEVMStorage, backend::ExecutionBackend}
};
use tokio::time::Instant;
use tracing::{info, debug, warn, error};


use crate::{SimulationResult, AddressBasedConflictGraph};

use super::{
    types::SimulatedTransaction, 
    address_based_conflict_graph::Transaction
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

    async fn _execute(&self, consensus_output: Vec<ExecutableEthereumBatch>) -> Vec<BatchDigest> {
        let mut now = Instant::now();
        let SimulationResult { digests, rw_sets } = self._simulate(consensus_output).await;
        let mut time = now.elapsed().as_millis();
        info!("Simulation took {} ms for {} transactions.", time, rw_sets.len());

        // rw_sets.clone().iter().for_each(|rw_set| {
        //     println!("rw_set: {:?}\n", rw_set);
        // });
        
        now = Instant::now();
        let scheduled_info = AddressBasedConflictGraph::construct(rw_sets)
            .hierarchcial_sort()
            .reorder()
            .extract_schedule();
        time = now.elapsed().as_millis();
        info!("Scheduling took {} ms.", time);

        let scheduled_tx_len = scheduled_info.scheduled_txs_len();
        let aborted_tx_len =  scheduled_info.aborted_txs_len();
        info!("Parallelism metric: {:?}", scheduled_info.parallism_metric());

        now = Instant::now();
        self._concurrent_commit(scheduled_info, 10).await;
        time = now.elapsed().as_millis();

        info!("Concurrent commit took {} ms for {} transactions.", time, scheduled_tx_len);
        info!("Abort rate: {:.2} ({}/{} aborted)", (aborted_tx_len as f64) * 100.0 / (scheduled_tx_len+aborted_tx_len) as f64, aborted_tx_len, scheduled_tx_len+aborted_tx_len);

        // println!("{} transactions are aborted.", aborted_tx_len);

        digests
    }
    
    //TODO: create Simulator having a thread pool for cpu-bound jobs.
    pub async fn _simulate(&self, consensus_output: Vec<ExecutableEthereumBatch>) -> SimulationResult {
        let snapshot = self.global_state.snapshot();
        
        let (digests, batches): (Vec<_>, Vec<_>) = consensus_output
            .iter()
            .map(|batch| (batch.digest().to_owned(), batch.data().to_owned()))
            .unzip();

        let tx_list = batches
            .into_iter()
            .flatten()
            .collect_vec();

        // Parallel simulation requires heavy cpu usages. 
        // CPU-bound jobs would make the I/O-bound tokio threads starve.
        // To this end, a separated thread pool need to be used for cpu-bound jobs.
        // a new thread is created, and a new thread pool is created on the thread. (specifically, rayon's thread pool is created)
        let result = tokio::task::spawn_blocking(move || {
            tx_list
                .par_iter()
                .filter_map(|tx| {
                    match crate::evm_utils::simulate_tx(tx, &snapshot) {
                        Ok(Some((effect, log, rw_set))) => {
                            
                            Some(SimulatedTransaction::new(0, tx.digest(), Some(rw_set), effect, log))
                        },
                        _ => {
                            warn!("fail to execute a transaction {}", tx.id());
                            None
                        },
                    }
                })
                .collect()
        }).await;

        match result {
            Ok(rw_sets) => {
                SimulationResult { digests, rw_sets }
            },
            Err(e) => {
                error!("fail to receive simulation result from the worker thread. {:?}", e);
                SimulationResult { digests, rw_sets: Vec::new() }
            }
        }
    }

    pub async fn _concurrent_commit(&self, scheduled_info: ScheduledInfo, chunk_size: usize) {
        let storage = self.global_state.clone();
        // let _storage = &storage;
        let scheduled_txs = scheduled_info.scheduled_txs;

        // Parallel simulation requires heavy cpu usages. 
        // CPU-bound jobs would make the I/O-bound tokio threads starve.
        // To this end, a separated thread pool need to be used for cpu-bound jobs.
        // a new thread is created, and a new thread pool is created on the thread. (specifically, rayon's thread pool is created)
        let _ = tokio::task::spawn_blocking(move || {
            let _storage = &storage;
            for txs_to_commit in scheduled_txs {
                txs_to_commit
                    .par_chunks(chunk_size)
                    .for_each(|txs| {
                        let effect = txs.into_iter().flat_map(|tx| tx.extract()).collect_vec();
                        _storage.apply_local_effect(effect)
                    })
            }
        }).await;

        // self.global_state.update_from(storage);
    }
    
}



pub struct ScheduledInfo {
    pub scheduled_txs: Vec<Vec<SimulatedTransaction>>,  
    pub aborted_txs: Vec<H256>
}

impl ScheduledInfo {

    pub fn from(tx_list: hashbrown::HashMap<H256, Rc<Transaction>>, aborted_txs: Vec<Rc<Transaction>>) -> Self {

        // group by sequence.
        let mut list = tx_list.into_iter()
            .map(|(_, tx)| {
                let tx = Self::_unwrap(tx);
                let tx_id = tx.id();
                let sequence = tx.sequence().to_owned();
                let (effects, logs) = tx.simulation_result();

                SimulatedTransaction::new(sequence, tx_id, None, effects, logs)
            })
            .collect::<Vec<SimulatedTransaction>>();

        // sort groups by sequence.
        list.sort_unstable_by_key(|tx| tx.seq());
        let mut scheduled_txs = Vec::<Vec<SimulatedTransaction>>::new(); 
        for (_key, txns) in &list.into_iter().group_by(|tx| tx.seq()) {
            scheduled_txs.push(txns.collect_vec());
        }
        
        let aborted_txs = aborted_txs.into_iter().map(|tx| tx.id()).collect::<Vec<H256>>();
        
        let mut count=0;
        scheduled_txs.iter().for_each(|vec| count += vec.len());
        debug!("{} scheduled transactions are scheduled group by {}", count, scheduled_txs.len());
        debug!("{} aborted transactions: {:?}", aborted_txs.len(), aborted_txs);

        Self { scheduled_txs, aborted_txs }
    }

    fn _unwrap(tx: Rc<Transaction>) -> Transaction {
        Rc::try_unwrap(tx).unwrap_or_else(|tx| 
            panic!("fail to unwrap transaction. (strong:{}, weak:{}", Rc::strong_count(&tx), Rc::weak_count(&tx))
        )
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



