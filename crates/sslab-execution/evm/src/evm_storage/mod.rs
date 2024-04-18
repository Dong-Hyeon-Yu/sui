pub mod backend;
mod evm_storage;

use std::{collections::BTreeMap, str::FromStr as _};

use ethers_core::{
    types::{H160, U256, U64},
    utils::hex,
};
use evm::backend::{MemoryAccount, MemoryVicinity};
pub use evm_storage::*;
use reth::primitives::{Address, Bytes};
use reth::revm::InMemoryDB;

use self::backend::{
    CAccount, CMemoryBackend, ConcurrentHashMap, InMemoryConcurrentDB, MemoryBackend,
};

pub type SerialEVMStorage = EvmStorage<MemoryBackend>;
pub type ConcurrentEVMStorage = EvmStorage<CMemoryBackend>;

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
    let mut state = BTreeMap::new();
    state.insert(
        H160::from_str(contract_addr).unwrap(),
        MemoryAccount {
            nonce: U256::one(),
            balance: U256::from(10000000),
            storage: BTreeMap::new(),
            code: hex::decode(bytecode).unwrap(),
        },
    );
    state.insert(
        H160::from_str(admin_acc).unwrap(),
        MemoryAccount {
            nonce: U256::one(),
            balance: U256::from(10000000),
            storage: BTreeMap::new(),
            code: Vec::new(),
        },
    );

    EvmStorage::new(
        U64::from(9),
        MemoryBackend::new(vicinity, state),
        BTreeMap::new(),
    )
}

pub fn concurrent_evm_storage(
    contract_addr: &str,
    bytecode: &str,
    admin_acc: &str,
) -> ConcurrentEVMStorage {
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
    let state = ConcurrentHashMap::default();
    state.pin().insert(
        H160::from_str(contract_addr).unwrap(),
        CAccount {
            nonce: U256::one(),
            balance: U256::from(10000000),
            storage: ConcurrentHashMap::default(),
            code: hex::decode(bytecode).unwrap(),
        },
    );
    state.pin().insert(
        H160::from_str(admin_acc).unwrap(),
        CAccount {
            nonce: U256::one(),
            balance: U256::from(10000000),
            storage: ConcurrentHashMap::default(),
            code: Vec::new(),
        },
    );

    EvmStorage::new(
        U64::from(9),
        CMemoryBackend::new(vicinity, state),
        BTreeMap::new(),
    )
}

pub fn cmemory_backend(contract_addr: &str, bytecode: &str, admin_acc: &str) -> CMemoryBackend {
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
    let state = ConcurrentHashMap::default();
    state.pin().insert(
        H160::from_str(contract_addr).unwrap(),
        CAccount {
            nonce: U256::one(),
            balance: U256::from(10000000),
            storage: ConcurrentHashMap::default(),
            code: hex::decode(bytecode).unwrap(),
        },
    );
    state.pin().insert(
        H160::from_str(admin_acc).unwrap(),
        CAccount {
            nonce: U256::one(),
            balance: U256::from(10000000),
            storage: ConcurrentHashMap::default(),
            code: Vec::new(),
        },
    );

    CMemoryBackend::new(vicinity, state)
}

pub fn memory_database(contract_addr: &str, bytecode: &str, admin_acc: &str) -> InMemoryDB {
    let mut db = InMemoryDB::default();

    let admin_acc_info = reth::revm::primitives::state::AccountInfo::new(
        reth::primitives::U256::MAX,
        0u64,
        reth::primitives::B256::ZERO,
        reth::revm::primitives::Bytecode::new(),
    );

    db.insert_account_info(Address::from_str(admin_acc).unwrap(), admin_acc_info);

    let reth_bytecode =
        reth::revm::primitives::Bytecode::new_raw(Bytes::from_str(bytecode).unwrap());
    let code_hash = reth_bytecode.hash_slow();
    let smallbank_contract = reth::revm::primitives::state::AccountInfo::new(
        reth::primitives::U256::MAX,
        0u64,
        code_hash,
        reth_bytecode,
    );

    db.insert_account_info(
        Address::from_str(contract_addr).unwrap(),
        smallbank_contract,
    );
    db
}

pub fn concurrent_memory_database(
    contract_addr: &str,
    bytecode: &str,
    admin_acc: &str,
) -> InMemoryConcurrentDB {
    let db = InMemoryConcurrentDB::default();

    let admin_acc_info = reth::revm::primitives::state::AccountInfo::new(
        reth::primitives::U256::MAX,
        0u64,
        reth::primitives::B256::ZERO,
        reth::revm::primitives::Bytecode::new(),
    );

    db.insert_account_info(Address::from_str(admin_acc).unwrap(), admin_acc_info);

    let reth_bytecode =
        reth::revm::primitives::Bytecode::new_raw(Bytes::from_str(bytecode).unwrap());
    let code_hash = reth_bytecode.hash_slow();
    let smallbank_contract = reth::revm::primitives::state::AccountInfo::new(
        reth::primitives::U256::MAX,
        0u64,
        code_hash,
        reth_bytecode,
    );

    db.insert_account_info(
        Address::from_str(contract_addr).unwrap(),
        smallbank_contract,
    );
    db
}
