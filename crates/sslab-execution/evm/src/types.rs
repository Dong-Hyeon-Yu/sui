use std::rc::Rc;
use enumn;
use ethers_core::types::{H256, U256, Bytes};
use ethers_core::types::{Address, transaction::eip2718::TypedTransaction};
use ethers_core::utils::rlp::Rlp;
use evm::{Runtime, Config, Context};
use fastcrypto::hash::Hash;
use narwhal_types::{BatchDigest, ConsensusOutput, ConsensusOutputDigest};
use serde::{Serialize, Deserialize};

use crate::transaction_validator::TxValidationError;

pub(crate) const DEFAULT_EVM_STACK_LIMIT:usize = 1024;
pub(crate) const DEFAULT_EVM_MEMORY_LIMIT:usize = usize::MAX; 

#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
pub struct EthereumTransaction(pub TypedTransaction);

impl EthereumTransaction {

    pub fn digest_u64(&self) -> u64 {
        u64::from_be_bytes(self.0.sighash()[2..10].try_into().ok().unwrap())
    }

    pub fn digest(&self) -> H256 {
        self.0.sighash()
    }

    pub fn encode(&self) -> Vec<u8> {
        self.0.rlp().to_vec()
    }

    pub fn from_json(bytes: &[u8]) -> Result<EthereumTransaction, TxValidationError> { 
        let tx: TypedTransaction = serde_json::from_slice(bytes).unwrap();

        Ok(EthereumTransaction(tx))
    }

    pub fn from_rlp(bytes: &[u8]) -> Result<EthereumTransaction, TxValidationError> {
        let rlp = Rlp::new(bytes);

        let (tx, _) = TypedTransaction::decode_signed(&rlp)?;

        Ok(EthereumTransaction(tx))
    }

    pub fn execution_part(&self, code :Vec<u8>) -> Runtime {
        
        let context = Context {
            caller: *self.0.from().unwrap(),
            address: *self.0.to_addr().unwrap(), //TODO: check this
            apparent_value: *self.0.value().unwrap(), //TODO: only for delegate call?
        };

        Runtime::new(
            Rc::new(code), 
            Rc::new(self.0.data().unwrap().to_vec().clone()),
            context,
            DEFAULT_EVM_STACK_LIMIT,
            DEFAULT_EVM_MEMORY_LIMIT
        )
    }

    pub fn to_addr(&self) -> Option<&Address> {
        self.0.to_addr()
    }

    pub fn caller(&self) -> Address {
        self.0.from().unwrap().clone()
    }

    pub fn value(&self) -> U256 {
        self.0.value().unwrap().clone()
    }

    pub fn data(&self) -> Option<&Bytes> {
        self.0.data() 
    }

    pub fn gas_limit(&self) -> u64 {
        self.0.gas().unwrap().clone().as_u64()
    }

    pub fn access_list(&self) -> Vec<(Address, Vec<H256>)> {
        match self.0.access_list() {
            Some(list) => list.clone().0.iter().map(|item| (item.address, item.storage_keys.clone())).collect(),
            None => vec![]
        }
    }
    pub fn nonce(&self) -> U256 {
        self.0.nonce().unwrap().clone()
    }
}

impl std::hash::Hash for EthereumTransaction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.sighash().hash(state);
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
pub struct IndexedEthereumTransaction {
    pub tx: EthereumTransaction,
    pub id: u64,
}

impl IndexedEthereumTransaction {
    pub fn new(tx: EthereumTransaction, id: u64) -> Self {
        Self { tx, id }
    }

    pub fn data(&self) -> &EthereumTransaction {
        &self.tx
    }

    pub fn digest(&self) -> H256 {
        self.tx.digest()
    }

    pub fn digest_u64(&self) -> u64 {
        self.tx.digest_u64()
    }
}


#[derive(Clone, Debug, Default)]
pub struct ExecutableEthereumBatch{
    digest: BatchDigest,
    data: Vec<EthereumTransaction>, 
}

impl ExecutableEthereumBatch {
    pub fn new(batch: Vec<EthereumTransaction>, digest: BatchDigest) -> ExecutableEthereumBatch {
        Self {
            data: batch,
            digest
        }
    }

    pub fn digest(&self) -> &BatchDigest {
        &self.digest
    }

    pub fn data(&self) -> &Vec<EthereumTransaction> {
        &self.data
    }
}

#[derive(Clone, Debug)]
pub struct ExecutableConsensusOutput {
    digest: ConsensusOutputDigest,
    data: Vec<ExecutableEthereumBatch>,
    timestamp: u64,
    round: u64,
    sub_dag_index: u64,
}

impl ExecutableConsensusOutput {
    pub fn new(data: Vec<ExecutableEthereumBatch>, consensus_output: &ConsensusOutput) -> ExecutableConsensusOutput {
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

    pub fn take_data(self) -> Vec<ExecutableEthereumBatch> {
        self.data
    }

    pub fn data(&self) -> &Vec<ExecutableEthereumBatch> {
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
        Self {
            digests
        }
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

#[derive(Clone, Debug)]
pub struct ChainConfig {
    config: Config
}

impl ChainConfig {
    pub fn new(chain_id: SpecId) -> Self {
        let config = match chain_id {
            SpecId::FRONTIER => Config::frontier(),
            // SpecId::FRONTIER_THAWING => Config::frontier_thawing(),
            // SpecId::HOMESTEAD => Config::homestead(),
            // SpecId::DAO_FORK => Config::dao_fork(),
            // SpecId::TANGERINE => Config::tangerine(),
            // SpecId::SPURIOUS_DRAGON => Config::spurious_dragon(),
            // SpecId::BYZANTIUM => Config::byzantium(),
            // SpecId::CONSTANTINOPLE => Config::constantinople(),
            // SpecId::PETERSBURG => Config::petersburg(),
            SpecId::ISTANBUL => Config::istanbul(),
            // SpecId::MUIR_GLACIER => Config::muir_glacier(),
            SpecId::BERLIN => Config::berlin(),
            SpecId::LONDON => Config::london(),
            // SpecId::ARROW_GLACIER => Config::arrow_glacier(),
            // SpecId::GRAY_GLACIER => Config::gray_glacier(),
            SpecId::MERGE => Config::merge(),
            SpecId::SHANGHAI => Config::shanghai(),
            // SpecId::CANCUN => Config::cancun(),
            SpecId::LATEST => Config::shanghai(),
            _ => panic!("SpecId is not supported")
        };

        Self {
            config
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

}
