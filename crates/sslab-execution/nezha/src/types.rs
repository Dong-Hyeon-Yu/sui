use core::panic;

use crate::address_based_conflict_graph::{KdgKey, Transaction};
use itertools::Itertools;
use narwhal_types::BatchDigest;
use reth::revm::primitives::{HashMap, HashSet, ResultAndState, State};
use sslab_execution::types::IndexedEthereumTransaction;

pub type Address = reth::revm::primitives::Address;
pub type Key = reth::revm::primitives::U256;
pub type RwSet = (
    HashMap<Address, HashSet<Key>>,
    HashMap<Address, HashSet<Key>>,
);

// SimulcationResult includes the batch digests and rw sets of each transctions in a ConsensusOutput.
#[derive(Clone, Debug, Default)]
pub struct SimulationResultV2 {
    pub digests: Vec<BatchDigest>,
    pub rw_sets: Vec<SimulatedTransactionV2>,
}

#[derive(Clone, Debug)]
pub struct SimulatedTransactionV2 {
    pub tx_id: u64,
    pub rw_set: Option<RwSet>,
    pub effects: State,
    pub raw_tx: IndexedEthereumTransaction,
}

impl SimulatedTransactionV2 {
    pub fn new(
        result: ResultAndState,
        read_set: HashMap<Address, HashSet<Key>>,
        write_set: HashMap<Address, HashSet<Key>>,
        raw_tx: IndexedEthereumTransaction,
    ) -> Self {
        let ResultAndState { result: _, state } = result;
        Self {
            tx_id: raw_tx.id,
            effects: state,
            rw_set: Some((read_set, write_set)),
            raw_tx,
        }
    }

    pub fn deconstruct(self) -> (u64, RwSet, State, IndexedEthereumTransaction) {
        (self.tx_id, self.rw_set.unwrap(), self.effects, self.raw_tx)
    }

    pub fn write_set(&self) -> Option<HashSet<KdgKey>> {
        match self.rw_set {
            Some((_, ref write_set)) => Some(
                write_set
                    .iter()
                    .flat_map(|(addr, keys)| {
                        keys.iter()
                            .map(|k| KdgKey {
                                address: *addr,
                                state_key: *k,
                            })
                            .collect_vec()
                    })
                    .collect::<HashSet<KdgKey>>(),
            ),
            None => None,
        }
    }
}

pub struct OptimisticInfo {
    raw_tx: IndexedEthereumTransaction,
    prev_write_keys: HashMap<Address, HashSet<Key>>,
    prev_read_keys: HashMap<Address, HashSet<Key>>,
}

pub struct ScheduledTransaction {
    pub seq: u64,
    pub tx_id: u64,
    pub effects: State,
    optimistic_info: Option<OptimisticInfo>,
}

impl ScheduledTransaction {
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn extract(&self) -> State {
        self.effects.clone()
    }

    #[allow(dead_code)] // this function is used in unit tests.
    pub(crate) fn id(&self) -> u64 {
        self.tx_id
    }

    pub fn rw_set(
        &self,
    ) -> (
        HashMap<Address, HashSet<Key>>,
        HashMap<Address, HashSet<Key>>,
    ) {
        match &self.optimistic_info {
            Some(info) => (info.prev_write_keys.clone(), info.prev_read_keys.clone()),
            None => panic!("No rw_set for this transaction"),
        }
    }

    pub fn raw_tx(&self) -> &IndexedEthereumTransaction {
        match &self.optimistic_info {
            Some(info) => &info.raw_tx,
            None => panic!("No raw_tx for this transaction"),
        }
    }
}

impl From<SimulatedTransactionV2> for ScheduledTransaction {
    fn from(tx: SimulatedTransactionV2) -> Self {
        let (tx_id, rw_set, effects, raw_tx) = tx.deconstruct();

        let optimistic_info = Some(OptimisticInfo {
            raw_tx,
            prev_write_keys: rw_set.1.clone(),
            prev_read_keys: rw_set.0.clone(),
        });

        Self {
            seq: 0,
            tx_id,
            effects,
            optimistic_info,
        }
    }
}

impl From<std::sync::Arc<Transaction>> for ScheduledTransaction {
    fn from(tx: std::sync::Arc<Transaction>) -> Self {
        let tx_id = tx.id();
        let seq = tx.sequence().to_owned();
        let raw_tx = tx.raw_tx().to_owned();

        let optimistic_info = Some(OptimisticInfo {
            raw_tx,
            prev_write_keys: tx.abort_info.read().prev_write_map().to_owned(),
            prev_read_keys: tx.abort_info.read().prev_read_map().to_owned(),
        });

        Self {
            seq,
            tx_id,
            effects: tx.simulation_result(),
            optimistic_info,
        }
    }
}

impl From<Transaction> for ScheduledTransaction {
    fn from(tx: Transaction) -> Self {
        let tx_id = tx.id();
        let seq = tx.sequence().to_owned();
        let raw_tx = tx.raw_tx().to_owned();

        let optimistic_info = Some(OptimisticInfo {
            raw_tx,
            prev_write_keys: tx.abort_info.read().prev_write_map().to_owned(),
            prev_read_keys: tx.abort_info.read().prev_read_map().to_owned(),
        });

        Self {
            seq,
            tx_id,
            effects: tx.simulation_result(),
            optimistic_info,
        }
    }
}
