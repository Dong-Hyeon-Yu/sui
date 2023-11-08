use std::{collections::BTreeMap, str::FromStr};

use ethers_core::{types::{U256, H160, U64}, utils::hex};
use evm::backend::{MemoryVicinity, MemoryAccount, MemoryBackend};

use crate::execution_storage::MemoryStorage;


pub const CONTRACT_BYTECODE: &str = include_str!("./DeployedSmallBank.bin");
pub const DEFAULT_CONTRACT_ADDRESS: &str = "0x1000000000000000000000000000000000000000";
pub const ADMIN_ADDRESS: &str = "0xe14de1592b52481b94b99df4e9653654e14fffb6";

pub fn default_memory_storage() -> MemoryStorage {
    let vicinity = MemoryVicinity { 
        gas_price: U256::zero(), 
        origin: H160::default(), 
        chain_id: U256::one(), 
        block_hashes: Vec::new(), 
        block_number: Default::default(), 
        block_coinbase: Default::default(), 
        block_timestamp: Default::default(), 
        block_difficulty: Default::default(), 
        block_gas_limit: Default::default(), 
        block_base_fee_per_gas: U256::zero(), //Gwei 
        block_randomness: None
    };
    let mut state = BTreeMap::new();
    state.insert(
        H160::from_str(DEFAULT_CONTRACT_ADDRESS).unwrap(),
        MemoryAccount {
            nonce: U256::one(),
            balance: U256::from(10000000),
            storage: BTreeMap::new(),
            code: hex::decode(CONTRACT_BYTECODE).unwrap(),
        }
    );
    state.insert(
        H160::from_str(ADMIN_ADDRESS).unwrap(),
        MemoryAccount {
            nonce: U256::one(),
            balance: U256::from(10000000),
            storage: BTreeMap::new(),
            code: Vec::new(),
        },
    );

    MemoryStorage::new(
        U64::from(9),
        MemoryBackend::new(vicinity, state),
        BTreeMap::new(),
    )
}