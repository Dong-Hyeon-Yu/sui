use ethers_core::types::H256;
use evm::{backend::{Apply, Log}, executor::stack::RwSet};
use itertools::Itertools;
use narwhal_types::BatchDigest;
use sslab_execution::types::EthereumTransaction;

// SimulcationResult includes the batch digests and rw sets of each transctions in a ConsensusOutput.
#[derive(Clone, Debug, Default)]
pub struct SimulationResult {
    pub digests: Vec<BatchDigest>,
    pub rw_sets: Vec<SimulatedTransaction>,
}

#[derive(Clone, Debug, Default)]
pub struct SimulatedTransaction {
    tx_id: H256,
    rw_set: Option<RwSet>,
    effects: Vec<Apply>,
    logs: Vec<Log>,
    raw_tx: EthereumTransaction
}

impl SimulatedTransaction {
    pub fn new(tx_id: H256, rw_set: Option<RwSet>, effects: Vec<Apply>, logs: Vec<Log>, raw_tx: EthereumTransaction) -> Self {
        Self { tx_id, rw_set, effects, logs, raw_tx }
    }

    pub fn deconstruct(self) -> (H256, Option<RwSet>, Vec<Apply>, Vec<Log>, EthereumTransaction) {
        (self.tx_id, self.rw_set, self.effects, self.logs, self.raw_tx)
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
}


pub struct ScheduledTransaction {
    pub seq: u64, 
    pub tx_id: H256,
    pub effects: Vec<Apply>,
    pub logs: Vec<Log>,
}

impl ScheduledTransaction {
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn extract(&self) -> Vec<Apply> {
        self.effects.clone()
    }

    #[allow(dead_code)] // this function is used in unit tests.
    pub(crate) fn id(&self) -> &H256 {
        &self.tx_id
    }
}

impl From<SimulatedTransaction> for ScheduledTransaction {
    fn from(tx: SimulatedTransaction) -> Self {
        let (tx_id, _, effects, logs, _) = tx.deconstruct();
        Self { seq: 0, tx_id, effects, logs }
    }
}