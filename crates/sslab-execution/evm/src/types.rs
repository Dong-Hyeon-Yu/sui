use enumn;
use fastcrypto::hash::Hash;
use narwhal_types::{BatchDigest, ConsensusOutput, ConsensusOutputDigest};
use reth::primitives::{TransactionSignedEcRecovered, B256};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexedEthereumTransaction {
    pub tx: TransactionSignedEcRecovered,
    pub id: u64,
}

impl IndexedEthereumTransaction {
    pub fn new(tx: TransactionSignedEcRecovered, id: u64) -> Self {
        Self { tx, id }
    }

    pub fn data(&self) -> &TransactionSignedEcRecovered {
        &self.tx
    }

    pub fn digest(&self) -> B256 {
        self.tx.hash()
    }

    pub fn digest_u64(&self) -> u64 {
        self.id
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Clone, Debug, Default)]
pub struct ExecutableEthereumBatch<T> {
    digest: BatchDigest,
    data: Vec<T>,
}

impl<T> ExecutableEthereumBatch<T> {
    pub fn new(batch: Vec<T>, digest: BatchDigest) -> Self {
        Self {
            data: batch,
            digest,
        }
    }

    pub fn digest(&self) -> &BatchDigest {
        &self.digest
    }

    pub fn data(&self) -> &Vec<T> {
        &self.data
    }

    pub fn take_data(self) -> Vec<T> {
        self.data
    }
}

#[derive(Clone, Debug)]
pub struct ExecutableConsensusOutput<T: Clone> {
    digest: ConsensusOutputDigest,
    data: Vec<ExecutableEthereumBatch<T>>,
    timestamp: u64,
    round: u64,
    sub_dag_index: u64,
}

impl<T: Clone> ExecutableConsensusOutput<T> {
    pub fn new(data: Vec<ExecutableEthereumBatch<T>>, consensus_output: &ConsensusOutput) -> Self {
        Self {
            digest: consensus_output.digest(),
            data,
            timestamp: consensus_output.sub_dag.commit_timestamp(),
            round: consensus_output.sub_dag.leader_round(),
            sub_dag_index: consensus_output.sub_dag.sub_dag_index,
        }
    }

    pub fn digest(&self) -> &ConsensusOutputDigest {
        &self.digest
    }

    pub fn take_data(self) -> Vec<ExecutableEthereumBatch<T>> {
        self.data
    }

    pub fn data(&self) -> &Vec<ExecutableEthereumBatch<T>> {
        &self.data
    }

    pub fn timestamp(&self) -> &u64 {
        &self.timestamp
    }

    pub fn round(&self) -> &u64 {
        &self.round
    }

    pub fn sub_dag_index(&self) -> &u64 {
        &self.sub_dag_index
    }
}

pub struct ExecutionResult {
    pub digests: Vec<BatchDigest>,
}

impl ExecutionResult {
    pub fn new(digests: Vec<BatchDigest>) -> Self {
        Self { digests }
    }

    pub fn iter(&self) -> impl Iterator<Item = &BatchDigest> {
        self.digests.iter()
    }
}

/// SpecId and their activation block
/// Information was obtained from: https://github.com/ethereum/execution-specs
#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, enumn::N)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[allow(non_camel_case_types)]
pub enum SpecId {
    FRONTIER = 0,         // Frontier	            0
    FRONTIER_THAWING = 1, // Frontier Thawing       200000
    HOMESTEAD = 2,        // Homestead	            1150000
    DAO_FORK = 3,         // DAO Fork	            1920000
    TANGERINE = 4,        // Tangerine Whistle	    2463000
    SPURIOUS_DRAGON = 5,  // Spurious Dragon        2675000
    BYZANTIUM = 6,        // Byzantium	            4370000
    CONSTANTINOPLE = 7,   // Constantinople         7280000 is overwritten with PETERSBURG
    PETERSBURG = 8,       // Petersburg             7280000
    ISTANBUL = 9,         // Istanbul	            9069000
    MUIR_GLACIER = 10,    // Muir Glacier	        9200000
    BERLIN = 11,          // Berlin	                12244000
    LONDON = 12,          // London	                12965000
    ARROW_GLACIER = 13,   // Arrow Glacier	        13773000
    GRAY_GLACIER = 14,    // Gray Glacier	        15050000
    MERGE = 15,           // Paris/Merge	        TBD (Depends on difficulty)
    SHANGHAI = 16,
    CANCUN = 17,
    LATEST = 18,
}

impl SpecId {
    pub fn try_from_u8(spec_id: u8) -> Option<Self> {
        Self::n(spec_id)
    }

    pub fn try_from_u256(spec_id: ethers_core::types::U256) -> Option<Self> {
        Self::n(spec_id.byte(0) as u8)
    }
}
