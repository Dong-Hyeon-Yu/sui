use std::{sync::Arc, rc::Rc};
use itertools::Itertools;
use parking_lot::RwLock;
use rayon::prelude::*;
use tracing::{info, debug, warn, error};

use crate::{
    types::{ExecutableEthereumBatch, ExecutionResult}, 
    executor::{EvmExecutionUtils, ParallelExecutable}, 
    execution_storage::{ExecutionBackend, MemoryStorage}, 
    execution_models::nezha::{
        address_based_conflict_graph::AddressBasedConflictGraph, 
        types::SimulationResult
    }
};

use super::{types::SimulatedTransaction, address_based_conflict_graph::{Transaction, FastHashMap}};

pub struct Nezha {
    global_state: Arc<RwLock<MemoryStorage>>,
}

impl ParallelExecutable for Nezha {
    fn execute(&self, consensus_output: Vec<ExecutableEthereumBatch>) -> ExecutionResult {
        info!("Simulation started.");
        let SimulationResult { digests, rw_sets } = self._simulate(consensus_output);
        info!("Simulation finished.");

        info!("Nezha started.");
        let scheduled_info = AddressBasedConflictGraph::construct(rw_sets)
            .hierarchcial_sort()
            .reorder()
            .extract_schedule();
        info!("Nezha finished.");

        info!("Concurrent commit started.");
        self._concurrent_commit(scheduled_info);
        info!("Concurrent commit finished.");

        ExecutionResult::new(digests)
    }
}

impl Nezha {
    pub fn new(global_state: Arc<RwLock<MemoryStorage>>) -> Self {
        Self {
            global_state,
        }
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
                    let mut executor = snapshot.executor(tx.gas_limit(), true);
                    match EvmExecutionUtils::execute_tx(tx, &snapshot, true) {
                        Ok(Some((effect, log))) => {
                            let rw_set = executor.rw_set().expect("rw_set must be exist in simulation mode.");
                            Some(SimulatedTransaction::new(tx.id(), Some(rw_set.to_owned()), effect, log))
                        },
                        _ => {
                            warn!("fail to execute a transaction {}", tx.id());
                            None
                        },
                    }
                })
                .collect();

            let _ = tx.send(result);
        }).join().ok();

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

        info!("{} transactions are aborted.", scheduled_info.aborted_txs.len());
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
}



