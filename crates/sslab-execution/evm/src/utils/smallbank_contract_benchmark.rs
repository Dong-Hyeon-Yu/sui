use ethers_providers::{MockProvider, Provider};
use reth::revm::InMemoryDB;

use crate::db::in_memory_db::InMemoryConcurrentDB;

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
