use core::panic;

use ethers_core::types::H256;
use evm::{backend::{Apply, Log}, executor::stack::RwSet};
use itertools::Itertools;
use narwhal_types::BatchDigest;
use sslab_execution::types::IndexedEthereumTransaction;

use crate::address_based_conflict_graph::Transaction;

// SimulcationResult includes the batch digests and rw sets of each transctions in a ConsensusOutput.
#[derive(Clone, Debug, Default)]
pub struct SimulationResult {
    pub digests: Vec<BatchDigest>,
    pub rw_sets: Vec<SimulatedTransaction>,
}

#[derive(Clone, Debug, Default)]
pub struct SimulatedTransaction {
    tx_id: u64,
    rw_set: Option<RwSet>,
    effects: Vec<Apply>,
    logs: Vec<Log>,
    raw_tx: IndexedEthereumTransaction,
}

impl SimulatedTransaction {
    pub fn new( rw_set: Option<RwSet>, effects: Vec<Apply>, logs: Vec<Log>, raw_tx: IndexedEthereumTransaction) -> Self {
        Self { tx_id: raw_tx.id, rw_set, effects, logs, raw_tx }
    }

    pub fn id(&self) -> u64 {
        self.tx_id
    }

    pub fn deconstruct(self) -> (u64, Option<RwSet>, Vec<Apply>, Vec<Log>, IndexedEthereumTransaction) {
        (self.raw_tx.id, self.rw_set, self.effects, self.logs, self.raw_tx)
    }

    pub fn write_set(&self) -> Option<hashbrown::HashSet<H256>> {
        match self.rw_set {
            Some(ref set) => {
                Some(set.writes()
                    .iter()
                    .flat_map(|(_, state)| state.keys().cloned().collect_vec())
                    .collect())
            },
            None => None
        }
    }

    pub fn read_set(&self) -> Option<hashbrown::HashSet<H256>> {
        match self.rw_set {
            Some(ref set) => {
                Some(set.reads()
                    .iter()
                    .flat_map(|(_, state)| state.keys().cloned().collect_vec())
                    .collect())
            },
            None => None
        }
    }

    pub fn raw_tx(&self) -> &IndexedEthereumTransaction {
        &self.raw_tx
    }
}


pub struct OptimisticInfo {
    raw_tx: IndexedEthereumTransaction,
    prev_write_keys: hashbrown::HashSet<H256>,
    prev_read_keys: hashbrown::HashSet<H256>,
}

pub struct ScheduledTransaction {
    pub seq: u64, 
    pub tx_id: u64,
    pub effects: Vec<Apply>,
    pub logs: Vec<Log>,
    optimistic_info: Option<OptimisticInfo>
}

impl ScheduledTransaction {
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn extract(&self) -> Vec<Apply> {
        self.effects.clone()
    }

    #[allow(dead_code)] // this function is used in unit tests.
    pub(crate) fn id(&self) -> u64 {
        self.tx_id
    }

    pub fn rw_set(&self) -> (hashbrown::HashSet<H256>, hashbrown::HashSet<H256>) {
        match &self.optimistic_info {
            Some(info) => (info.prev_write_keys.clone(), info.prev_read_keys.clone()),
            None => panic!("No rw_set for this transaction")
        }
    }

    pub fn raw_tx(&self) -> &IndexedEthereumTransaction {
        match &self.optimistic_info {
            Some(info) => &info.raw_tx,
            None => panic!("No raw_tx for this transaction")
        }
    }
}

impl From<SimulatedTransaction> for ScheduledTransaction {
    fn from(tx: SimulatedTransaction) -> Self {
        let (tx_id, rw_set, effects, logs, raw_tx) = tx.deconstruct();
        let optimistic_info = match rw_set {
            Some(rw_set) => {
                let write_keys = rw_set.writes()
                    .iter()
                    .flat_map(|(_, state)| state.keys().cloned().collect_vec())
                    .collect();
                let read_keys = rw_set.reads()
                    .iter()
                    .flat_map(|(_, state)| state.keys().cloned().collect_vec())
                    .collect();
                Some(OptimisticInfo { raw_tx, prev_write_keys: write_keys, prev_read_keys: read_keys })
            },
            None => None
        };
        Self { seq: 0, tx_id, effects, logs, optimistic_info }
    }
}


impl From<std::sync::Arc<Transaction>> for ScheduledTransaction {
    fn from(tx: std::sync::Arc<Transaction>) -> Self {
        let tx_id = tx.id();
        let seq = tx.sequence().to_owned();
        let raw_tx = tx.raw_tx().to_owned();

        let optimistic_info = Some(OptimisticInfo {
            raw_tx, 
            prev_write_keys: tx.abort_info.read().write_keys().to_owned(), 
            prev_read_keys: tx.abort_info.read().read_keys().to_owned()
        });

        let (effects, logs) = tx.simulation_result();

        Self { seq, tx_id, effects, logs, optimistic_info }
    }
}

impl From<Transaction> for ScheduledTransaction {
    fn from(tx: Transaction) -> Self {
        let tx_id = tx.id();
        let seq = tx.sequence().to_owned();
        let raw_tx = tx.raw_tx().to_owned();

        let optimistic_info = Some(OptimisticInfo {
            raw_tx, 
            prev_write_keys: tx.abort_info.read().write_keys().to_owned(), 
            prev_read_keys: tx.abort_info.read().read_keys().to_owned()
        });

        let (effects, logs) = tx.simulation_result();

        Self { seq, tx_id, effects, logs, optimistic_info }
    }
}