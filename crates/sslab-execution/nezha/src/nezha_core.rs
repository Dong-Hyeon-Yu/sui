use std::{sync::Arc, rc::Rc};
use itertools::Itertools;
use narwhal_types::BatchDigest;
use rayon::prelude::*;
use sslab_execution::{
    types::{ExecutableEthereumBatch, ExecutionResult}, 
    executor::{Executable, EvmExecutionUtils}, 
    evm_storage::{ConcurrentEVMStorage, backend::ExecutionBackend}
};
use tokio::time::Instant;
use tracing::{info, debug, warn, error};


use crate::{SimulationResult, AddressBasedConflictGraph};

use super::{
    types::SimulatedTransaction, 
    address_based_conflict_graph::{Transaction, FastHashMap}
};


impl Executable for Nezha {
    fn execute(&self, consensus_output: Vec<ExecutableEthereumBatch>) {

        let _ = self.inner.prepare_execution(consensus_output);
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

    fn prepare_execution(&self, consensus_output: Vec<ExecutableEthereumBatch>) -> ExecutionResult {

        let mut result = vec![];
        let mut target = consensus_output;
    
        while !target.is_empty() {
            let split_idx = std::cmp::min(self.concurrency_level, target.len());
            let remains: Vec<ExecutableEthereumBatch> = target.split_off(split_idx);

            result.extend(self._execute(target));

            target = remains;
        } 
        
        ExecutionResult::new(result)
    }

    fn _execute(&self, consensus_output: Vec<ExecutableEthereumBatch>) -> Vec<BatchDigest> {
        let mut now = Instant::now();
        let SimulationResult { digests, rw_sets } = self._simulate(consensus_output);
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
        self._concurrent_commit(scheduled_info);
        time = now.elapsed().as_millis();

        info!("Concurrent commit took {} ms for {} transactions.", time, scheduled_tx_len);
        info!("Abort rate: {:.2} ({}/{} aborted)", (aborted_tx_len as f64) * 100.0 / (scheduled_tx_len+aborted_tx_len) as f64, aborted_tx_len, scheduled_tx_len+aborted_tx_len);

        // println!("{} transactions are aborted.", aborted_tx_len);

        digests
    }
    
    //TODO: create Simulator having a thread pool for cpu-bound jobs.
    pub fn _simulate(&self, consensus_output: Vec<ExecutableEthereumBatch>) -> SimulationResult {
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
        let (tx, rx) = std::sync::mpsc::channel::<Vec<SimulatedTransaction>>();

        std::thread::spawn(move || {
            let result : Vec<SimulatedTransaction> = tx_list
                .par_iter()
                .filter_map(|tx| {
                    match EvmExecutionUtils::simulate_tx(tx, &snapshot) {
                        Ok(Some((effect, log, rw_set))) => {
                            
                            Some(SimulatedTransaction::new(tx.id(), Some(rw_set), effect, log))
                        },
                        _ => {
                            warn!("fail to execute a transaction {}", tx.id());
                            None
                        },
                    }
                })
                .collect();

            let _ = tx.send(result);
        }).join().expect("fail to join the simulation thread.");

        match rx.recv() {
            Ok(rw_sets) => {
                SimulationResult { digests, rw_sets }
            },
            Err(e) => {
                error!("fail to receive simulation result from the worker thread. {:?}", e);
                SimulationResult { digests, rw_sets: Vec::new() }
            }
        }
    }

    pub fn _concurrent_commit(&self, scheduled_info: ScheduledInfo) {
        let storage = self.global_state.clone();
        // let _storage = &storage;
        let scheduled_txs = scheduled_info.scheduled_txs;

        // Parallel simulation requires heavy cpu usages. 
        // CPU-bound jobs would make the I/O-bound tokio threads starve.
        // To this end, a separated thread pool need to be used for cpu-bound jobs.
        // a new thread is created, and a new thread pool is created on the thread. (specifically, rayon's thread pool is created)
        std::thread::spawn(move || {
            let _storage = &storage;
            for txs_to_commit in scheduled_txs {
                txs_to_commit
                    .par_iter()
                    .for_each(|tx| {
                        let effect = tx.extract();
                        _storage.apply_local_effect(effect)
                    })
            }
        }).join().expect("fail to commit concurrently.");

        // self.global_state.update_from(storage);
    }
    
}



pub struct ScheduledInfo {
    pub scheduled_txs: Vec<Vec<SimulatedTransaction>>,  
    pub aborted_txs: Vec<u64>
}

impl ScheduledInfo {

    pub fn from(tx_list: FastHashMap<u64, Rc<Transaction>>, aborted_txs: Vec<Rc<Transaction>>) -> Self {
        let mut buffer = FastHashMap::default();

        // group by sequence.
        tx_list.into_iter()
            .for_each(|(_, tx)| {
                let tx = Self::_unwrap(tx);
                let tx_id = tx.id();
                let sequence = tx.sequence().to_owned();
                let (effects, logs) = tx.simulation_result();

                let tx: SimulatedTransaction = SimulatedTransaction::new(tx_id, None, effects, logs);
                buffer.entry(sequence).or_insert_with(Vec::new)
                    .push(tx);
            });

        // sort groups by sequence.
        let scheduled_txs = buffer.keys().sorted()
            .map(|seq| {
                buffer.get(seq).unwrap().to_owned()
            })
            .collect_vec();
        let aborted_txs = aborted_txs.into_iter().map(|tx| tx.id()).collect_vec();
        
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

    pub fn parallism_metric(&self) -> (f64, usize, usize) {
        let max_width = self.scheduled_txs.iter().map(|vec| vec.len()).max().unwrap_or(0);
        let depth = self.scheduled_txs.len();
        let average_width = self.scheduled_txs.iter().map(|vec| vec.len()).sum::<usize>() as f64 / depth as f64;

        (average_width, max_width, depth)
    }
}



