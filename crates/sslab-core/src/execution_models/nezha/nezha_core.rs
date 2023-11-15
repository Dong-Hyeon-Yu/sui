use std::{sync::Arc, rc::Rc};
use itertools::Itertools;
use narwhal_types::BatchDigest;
use parking_lot::RwLock;
use rayon::prelude::*;
use tokio::{time::Instant, sync::mpsc::Sender};
use tracing::{info, debug, warn, error};

use crate::{
    types::{ExecutableEthereumBatch, ExecutionResult}, 
    executor::{EvmExecutionUtils, Executable}, 
    execution_storage::{ExecutionBackend, MemoryStorage}, 
    execution_models::nezha::{
        address_based_conflict_graph::AddressBasedConflictGraph, 
        types::SimulationResult
    }
};

use super::{types::SimulatedTransaction, address_based_conflict_graph::{Transaction, FastHashMap}};

pub struct Nezha {
    inner: ConcurrencyLevelManager,
}

#[async_trait::async_trait]
impl Executable for Nezha {
    async fn execute(&mut self, consensus_output: Vec<ExecutableEthereumBatch>, tx_execute_notification: &mut Sender<ExecutionResult>) {

        let result = self.inner.prepare_execution(consensus_output);
        let _ = tx_execute_notification.send(result).await;
    }
}

impl Nezha {
    pub fn new(global_state: Arc<RwLock<MemoryStorage>>, concurrency_level: usize) -> Self {
        Self {
            inner: ConcurrencyLevelManager::new(global_state, concurrency_level),
        }
    }
}

struct ConcurrencyLevelManager {
    concurrency_level: usize,
    global_state: Arc<RwLock<MemoryStorage>>
}

impl ConcurrencyLevelManager {
    
    fn new(global_state: Arc<RwLock<MemoryStorage>>, concurrency_level: usize) -> Self {
        Self {
            concurrency_level,
            global_state
        }
    }

    fn prepare_execution(&mut self, consensus_output: Vec<ExecutableEthereumBatch>) -> ExecutionResult {

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

        now = Instant::now();
        let scheduled_info = AddressBasedConflictGraph::construct(rw_sets)
            .hierarchcial_sort()
            .reorder()
            .extract_schedule();
        time = now.elapsed().as_millis();
        info!("Scheduling took {} ms.", time);

        let scheduled_tx_len = scheduled_info.scheduled_txs_len();
        let aborted_tx_len =  scheduled_info.aborted_txs_len();

        now = Instant::now();
        self._concurrent_commit(scheduled_info);
        time = now.elapsed().as_millis();

        info!("Concurrent commit took {} ms for {} transactions.", time, scheduled_tx_len);
        info!("{} transactions are aborted.", aborted_tx_len);

        digests
    }
    
    //TODO: create Simulator having a thread pool for cpu-bound jobs.
    fn _simulate(&self, consensus_output: Vec<ExecutableEthereumBatch>) -> SimulationResult {
        let local_state = self.global_state.read();

        let snapshot = local_state.snapshot();
        
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
                    match EvmExecutionUtils::execute_tx(tx, &snapshot, true) {
                        Ok(Some((effect, log, rw_set))) => {
                            
                            Some(SimulatedTransaction::new(tx.id(), Some(rw_set.unwrap()), effect, log))
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

    fn _concurrent_commit(&self, scheduled_info: ScheduledInfo) {
        let mut storage = self.global_state.write();

        scheduled_info.scheduled_txs.into_iter()
            .flatten()
            .for_each(|tx| {
                let (_, _, effects, logs) = tx.deconstruct();
                storage.apply_local_effect(effects, logs);
            });

        // let (tx, rx) = std::sync::mpsc::channel::<Vec<SimulatedTransaction>>();

        // let thread = std::thread::spawn(move || {
        //     for target_txs in scheduled_info.scheduled_txs.iter() {

        //         target_txs.par_iter()
        //             .for_each(|tx| {
        //                 let (_, _, effects, logs) = tx.deconstruct();
        //                 self.global_state.write().apply_local_effect(effects, logs);  // TODO: address를 key로 하는 concurrent map을 사용해야함.
        //         })
        //     }
        // });
    }
}



pub(crate) struct ScheduledInfo {
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

    fn scheduled_txs_len(&self) -> usize {
        self.scheduled_txs.iter().map(|vec| vec.len()).sum()
    }

    fn aborted_txs_len(&self) -> usize {
        self.aborted_txs.len()
    }
}



