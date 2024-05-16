use std::str::FromStr;

use ethers_providers::{MockProvider, Provider};
use reth::{
    primitives::{Address, Bytes},
    revm::{interpreter::analysis::to_analysed, InMemoryDB},
};

use crate::db::{in_memory_db::InMemoryConcurrentDB, ThreadSafeCacheState};

use super::{
    in_memory_db_utils,
    test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID},
};

pub const CONTRACT_BYTECODE: &str = include_str!("./DeployedSmallBank.bin");
pub const DEFAULT_CONTRACT_ADDRESS: &str = "0x1000000000000000000000000000000000000000";
pub const ADMIN_ADDRESS: &str = "0xe14de1592b52481b94b99df4e9653654e14fffb6";

pub fn memory_database() -> InMemoryDB {
    in_memory_db_utils::memory_database(DEFAULT_CONTRACT_ADDRESS, CONTRACT_BYTECODE, ADMIN_ADDRESS)
}

pub fn concurrent_memory_database() -> InMemoryConcurrentDB {
    in_memory_db_utils::concurrent_memory_database(
        DEFAULT_CONTRACT_ADDRESS,
        CONTRACT_BYTECODE,
        ADMIN_ADDRESS,
    )
}

pub fn get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

pub fn cache_state_with_smallbank_contract() -> ThreadSafeCacheState {
    let state = ThreadSafeCacheState::default();

    let reth_bytecode =
        reth::revm::primitives::Bytecode::new_raw(Bytes::from_str(CONTRACT_BYTECODE).unwrap());
    let code_hash = reth_bytecode.hash_slow();
    let smallbank_contract = reth::revm::primitives::state::AccountInfo::new(
        reth::primitives::U256::MAX,
        0u64,
        code_hash,
        to_analysed(reth_bytecode),
    );

    state.insert_account(
        Address::from_str(DEFAULT_CONTRACT_ADDRESS).unwrap(),
        smallbank_contract,
    );
    state
}
