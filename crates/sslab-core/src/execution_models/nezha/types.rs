use evm::{backend::{Apply, Log}, executor::stack::RwSet};
use narwhal_types::BatchDigest;

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
}

impl SimulatedTransaction {
    pub fn new(tx_id: u64, rw_set: Option<RwSet>, effects: Vec<Apply>, logs: Vec<Log>) -> Self {
        Self { tx_id, rw_set, effects, logs }
    }

    pub fn deconstruct(self) -> (u64, Option<RwSet>, Vec<Apply>, Vec<Log>) {
        (self.tx_id, self.rw_set, self.effects, self.logs)
    }

    #[allow(dead_code)] // this function is used in unit tests.
    pub(crate) fn id(&self) -> &u64 {
        &self.tx_id
    }
}
