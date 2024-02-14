use std::sync::Arc;
use ethers_core::types::H256;
use itertools::Itertools;
use narwhal_types::BatchDigest;
use rayon::prelude::*;
use sslab_execution::{
    types::{ExecutableEthereumBatch, ExecutionResult, EthereumTransaction}, 
    executor::Executable, 
    evm_storage::{ConcurrentEVMStorage, backend::ExecutionBackend}
};
use tracing::warn;

use crate::{SimulationResult, AddressBasedConflictGraph, types::ScheduledTransaction};

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

    async fn _unpack_batches(consensus_output: Vec<ExecutableEthereumBatch>) -> (Vec<BatchDigest>, Vec<EthereumTransaction>){

        let (send, recv) = tokio::sync::oneshot::channel();

        rayon::spawn(move || {

            let (digests, batches): (Vec<_>, Vec<_>) = consensus_output
                .par_iter()
                .map(|batch| {
                    // let txs = batch.data()
                    //     .into_iter()
                    //     .map(|tx| Arc::new(tx.to_owned()))
                    //     .collect_vec();

                    (batch.digest().to_owned(), batch.data().to_owned())
                })
                .unzip();

            let tx_list = batches
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();

            let _ = send.send((digests, tx_list)).unwrap();
        });

        recv.await.unwrap()
    }

    pub async fn _execute(&self, consensus_output: Vec<ExecutableEthereumBatch>) -> Vec<BatchDigest> {
        
        let (digests, mut tx_list) = Self::_unpack_batches(consensus_output).await;

        while tx_list.len() > 0 {
            let rw_sets = self._simulate(tx_list).await;
        
            let (scheduled_info, aborted_txs) = AddressBasedConflictGraph::par_construct(rw_sets).await
                .hierarchcial_sort()
                .reorder()
                .par_extract_schedule().await;
    
            self._concurrent_commit(scheduled_info, 1).await;

            tx_list = aborted_txs;
        }
        

        digests
    }

    pub async fn simulate(&self, consensus_output: Vec<ExecutableEthereumBatch>) -> SimulationResult {
        let (digests, tx_list) = Self::_unpack_batches(consensus_output).await;
        let rw_sets = self._simulate(tx_list).await;

        SimulationResult {digests, rw_sets}
    }
    

    async fn _simulate(&self, tx_list: Vec<EthereumTransaction>) -> Vec<SimulatedTransaction> {
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
                    match crate::evm_utils::simulate_tx(&tx, snapshot.as_ref()) {
                        Ok(Some((effect, log, rw_set))) => {
                            
                            Some(SimulatedTransaction::new(tx.digest(), Some(rw_set), effect, log, tx))
                        },
                        _ => {
                            warn!("fail to execute a transaction {}", tx.id());
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

    pub async fn _concurrent_commit(&self, scheduled_info: ScheduledInfo, chunk_size: usize) {
        let storage = self.global_state.clone();
        let scheduled_txs = scheduled_info.scheduled_txs;

        // Parallel simulation requires heavy cpu usages. 
        // CPU-bound jobs would make the I/O-bound tokio threads starve.
        // To this end, a separated thread pool need to be used for cpu-bound jobs.
        // a new thread is created, and a new thread pool is created on the thread. (specifically, rayon's thread pool is created)
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let _storage = &storage;
            for txs_to_commit in scheduled_txs {
                txs_to_commit
                    .par_chunks(chunk_size)
                    .for_each(|txs| {
                        let effect = txs.into_iter().flat_map(|tx| tx.extract()).collect_vec();
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
    pub aborted_tx_num: usize,
}

impl ScheduledInfo {

    pub fn from(tx_list: hashbrown::HashMap<H256, Arc<Transaction>>, aborted_tx_num: usize) -> Self {

        // group by sequence.
        let mut list = tx_list.into_iter()
            .map(|(_, tx)| {
                let tx = Self::_unwrap(tx);
                let tx_id = tx.id();
                let seq = tx.sequence().to_owned();
                let (effects, logs) = tx.simulation_result();

                ScheduledTransaction {seq, tx_id, effects, logs }
            })
            .collect::<Vec<ScheduledTransaction>>();

        // sort groups by sequence.
        list.sort_unstable_by_key(|tx| tx.seq());
        let mut scheduled_txs = Vec::<Vec<ScheduledTransaction>>::new(); 
        for (_key, txns) in &list.into_iter().group_by(|tx| tx.seq()) {
            scheduled_txs.push(txns.collect_vec());
        }
        
        Self { scheduled_txs, aborted_tx_num }
    }


    pub fn par_from(tx_list: hashbrown::HashMap<H256, Arc<Transaction>>, aborted_tx_num: usize) -> Self {

        // group by sequence.
        let mut list = tx_list.into_par_iter()
            .map(|(_, tx)| {
                let tx = Self::_unwrap(tx);
                let tx_id = tx.id();
                let seq = tx.sequence().to_owned();
                let (effects, logs) = tx.simulation_result();

                ScheduledTransaction {seq, tx_id, effects, logs }
            })
            .collect::<Vec<ScheduledTransaction>>();

        // sort groups by sequence.
        list.sort_unstable_by_key(|tx| tx.seq());
        let mut scheduled_txs = Vec::<Vec<ScheduledTransaction>>::new(); 
        for (_key, txns) in &list.into_iter().group_by(|tx| tx.seq()) {
            scheduled_txs.push(txns.collect_vec());
        }
        
        Self { scheduled_txs, aborted_tx_num }
    }

    fn _unwrap(tx: Arc<Transaction>) -> Transaction {
        Arc::try_unwrap(tx).unwrap_or_else(|tx| 
            panic!("fail to unwrap transaction. (strong:{}, weak:{}", Arc::strong_count(&tx), Arc::weak_count(&tx))
        )
    }

    pub fn scheduled_txs_len(&self) -> usize {
        self.scheduled_txs.iter().map(|vec| vec.len()).sum()
    }

    pub fn aborted_txs_len(&self) -> usize {
        self.aborted_tx_num
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

