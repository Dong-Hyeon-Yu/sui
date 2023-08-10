use std::rc::Rc;
use std::sync::Arc;
use enumn;
use ethers_core::types::Address;
use ethers_core::types::transaction::eip2718::TypedTransaction;
use ethers_core::utils::keccak256;
use evm::{Runtime, Config, Context};
use narwhal_types::Certificate;
use once_cell::sync::OnceCell;
use serde::{Serialize, Deserialize};
use sui_types::digests::TransactionDigest;
use crate::executor::{DEFAULT_EVM_MEMORY_LIMIT, DEFAULT_EVM_STACK_LIMIT};

// pub type EthereumTransaction = Transaction;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct EthereumTransaction(TypedTransaction);

impl EthereumTransaction {

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

    pub fn to_addr(&self) -> &Address {
        self.0.to_addr().unwrap()
    }
}


#[derive(Clone, Debug, Default)]
pub struct ExecutableEthereumTransaction{
    digest: OnceCell<TransactionDigest>,
    data: Vec<EthereumTransaction>, 
    certificate: Arc<Certificate>,
}

impl ExecutableEthereumTransaction {
    pub fn new(batch: Vec<EthereumTransaction>, cert: Arc<Certificate>) -> ExecutableEthereumTransaction {
        Self {
            data: batch,
            certificate: cert,
            digest: OnceCell::new()
        }
    }

    pub fn digest(&self) -> &TransactionDigest {
        self.digest.get_or_init(|| {
            let mut _bytes: Vec<u8> = Vec::new();
            
            for tx in &self.data {
                _bytes.extend(tx.0.rlp().to_vec());
            }
            TransactionDigest::new(keccak256(&_bytes))
        })
    }

    pub fn data(&self) -> &Vec<EthereumTransaction> {
        &self.data
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

#[derive(Clone)]
pub struct ChainConfig {
    chain_id: SpecId,
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
            chain_id,
            config
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

}
