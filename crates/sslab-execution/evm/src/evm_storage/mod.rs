pub mod backend;
mod evm_storage;

use std::{collections::BTreeMap, str::FromStr as _};

use ethers_core::{
    types::{H160, U256, U64},
    utils::hex,
};
use evm::backend::{MemoryAccount, MemoryVicinity};
pub use evm_storage::*;

use self::backend::MemoryBackend;

pub type SerialEVMStorage = EvmStorage<MemoryBackend>;

pub fn memory_storage(contract_addr: &str, bytecode: &str, admin_acc: &str) -> SerialEVMStorage {
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
        block_randomness: None,
    };
    let mut state = hashbrown::HashMap::default();
    state.insert(
        H160::from_str(contract_addr).unwrap(),
        MemoryAccount {
            nonce: U256::one(),
            balance: U256::from(10000000),
            storage: hashbrown::HashMap::default(),
            code: hex::decode(bytecode).unwrap(),
        },
    );
    state.insert(
        H160::from_str(admin_acc).unwrap(),
        MemoryAccount {
            nonce: U256::one(),
            balance: U256::from(10000000),
            storage: hashbrown::HashMap::default(),
            code: Vec::new(),
        },
    );

    EvmStorage::new(
        U64::from(9),
        MemoryBackend::new(vicinity, state),
        BTreeMap::new(),
    )
}
