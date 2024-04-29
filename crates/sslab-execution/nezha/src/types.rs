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

#[derive(Debug)]
pub struct ScheduledTransaction {
    pub seq: u64,
    pub tx_id: u64,
    pub effects: State,
}

impl ScheduledTransaction {
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn extract(self) -> State {
        self.effects
    }

    #[allow(dead_code)] // this function is used in unit tests.
    pub(crate) fn id(&self) -> u64 {
        self.tx_id
    }
}

impl From<SimulatedTransactionV2> for ScheduledTransaction {
    fn from(tx: SimulatedTransactionV2) -> Self {
        let (tx_id, _rw_set, effects, _raw_tx) = tx.deconstruct();

        Self {
            seq: 0,
            tx_id,
            effects,
        }
    }
}

impl From<std::sync::Arc<Transaction>> for ScheduledTransaction {
    fn from(tx: std::sync::Arc<Transaction>) -> Self {
        let tx_id = tx.id();
        let seq = tx.sequence().to_owned();

        Self {
            seq,
            tx_id,
            effects: tx.simulation_result(),
        }
    }
}

impl From<Transaction> for ScheduledTransaction {
    fn from(tx: Transaction) -> Self {
        let tx_id = tx.id();
        let seq = tx.sequence().to_owned();

        Self {
            seq,
            tx_id,
            effects: tx.simulation_result(),
        }
    }
}
