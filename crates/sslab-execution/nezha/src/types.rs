use ethers_core::types::H256;
use evm::{
    backend::{Apply, Log},
    executor::stack::RwSet,
};
use narwhal_types::BatchDigest;

// SimulcationResult includes the batch digests and rw sets of each transctions in a ConsensusOutput.
#[derive(Clone, Debug, Default)]
pub struct SimulationResult {
    pub digests: Vec<BatchDigest>,
    pub rw_sets: Vec<SimulatedTransaction>,
}

#[derive(Clone, Debug, Default)]
pub struct SimulatedTransaction {
    seq: u64,
    tx_id: H256,
    rw_set: Option<RwSet>,
    effects: Vec<Apply>,
    logs: Vec<Log>,
}

impl SimulatedTransaction {
    pub fn new(
        seq: u64,
        tx_id: H256,
        rw_set: Option<RwSet>,
        effects: Vec<Apply>,
        logs: Vec<Log>,
    ) -> Self {
        Self {
            seq,
            tx_id,
            rw_set,
            effects,
            logs,
        }
    }

    pub fn deconstruct(self) -> (H256, Option<RwSet>, Vec<Apply>, Vec<Log>) {
        (self.tx_id, self.rw_set, self.effects, self.logs)
    }

    pub fn extract(&self) -> Vec<Apply> {
        self.effects.clone()
    }

    #[allow(dead_code)] // this function is used in unit tests.
    pub(crate) fn id(&self) -> &H256 {
        &self.tx_id
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }
}
