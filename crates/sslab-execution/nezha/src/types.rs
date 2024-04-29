use crate::address_based_conflict_graph::{KdgKey, Transaction};
use evm::{
    backend::{Apply, Log},
    // executor::stack::RwSet,
};
use itertools::Itertools;
use narwhal_types::BatchDigest;
use sslab_execution::types::IndexedEthereumTransaction;

pub type Address = ethers_core::types::H160;
pub type Key = ethers_core::types::H256;
pub type RwSet = (
    hashbrown::HashMap<Address, hashbrown::HashSet<Key>>,
    hashbrown::HashMap<Address, hashbrown::HashSet<Key>>,
);

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
    pub fn new(
        rw_set: evm::executor::stack::RwSet,
        effects: Vec<Apply>,
        logs: Vec<Log>,
        raw_tx: IndexedEthereumTransaction,
    ) -> Self {
        let write_set = rw_set
            .writes()
            .iter()
            .map(|(addr, keys)| {
                (
                    *addr,
                    keys.iter()
                        .map(|(k, _)| *k)
                        .collect::<hashbrown::HashSet<Key>>(),
                )
            })
            .collect::<hashbrown::HashMap<Address, hashbrown::HashSet<Key>>>();
        let read_set = rw_set
            .reads()
            .iter()
            .map(|(addr, keys)| {
                (
                    *addr,
                    keys.iter()
                        .map(|(k, _)| *k)
                        .collect::<hashbrown::HashSet<Key>>(),
                )
            })
            .collect::<hashbrown::HashMap<Address, hashbrown::HashSet<Key>>>();
        Self {
            tx_id: raw_tx.id,
            rw_set: Some((read_set, write_set)),
            effects,
            logs,
            raw_tx,
        }
    }

    pub fn id(&self) -> u64 {
        self.tx_id
    }

    pub fn deconstruct(
        self,
    ) -> (
        u64,
        Option<RwSet>,
        Vec<Apply>,
        Vec<Log>,
        IndexedEthereumTransaction,
    ) {
        (
            self.raw_tx.id,
            self.rw_set,
            self.effects,
            self.logs,
            self.raw_tx,
        )
    }

    pub fn write_set(&self) -> Option<hashbrown::HashSet<KdgKey>> {
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
                    .collect::<hashbrown::HashSet<KdgKey>>(),
            ),
            None => None,
        }
    }
}

#[derive(Debug)]
pub struct ScheduledTransaction {
    pub seq: u64,
    pub tx_id: u64,
    pub effects: Vec<Apply>,
}

impl ScheduledTransaction {
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn extract(self) -> Vec<Apply> {
        self.effects
    }

    #[allow(dead_code)] // this function is used in unit tests.
    pub(crate) fn id(&self) -> u64 {
        self.tx_id
    }
}

impl From<SimulatedTransaction> for ScheduledTransaction {
    fn from(tx: SimulatedTransaction) -> Self {
        let (tx_id, _, effects, _, _) = tx.deconstruct();

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

        let (effects, _) = tx.simulation_result();

        Self {
            seq,
            tx_id,
            effects,
        }
    }
}

impl From<Transaction> for ScheduledTransaction {
    fn from(tx: Transaction) -> Self {
        let tx_id = tx.id();
        let seq = tx.sequence().to_owned();

        let (effects, _) = tx.simulation_result();

        Self {
            seq,
            tx_id,
            effects,
        }
    }
}
