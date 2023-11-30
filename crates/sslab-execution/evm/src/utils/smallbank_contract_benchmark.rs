use crate::evm_storage::{self, SerialEVMStorage, ConcurrentEVMStorage};


pub const CONTRACT_BYTECODE: &str = include_str!("./DeployedSmallBank.bin");
pub const DEFAULT_CONTRACT_ADDRESS: &str = "0x1000000000000000000000000000000000000000";
pub const ADMIN_ADDRESS: &str = "0xe14de1592b52481b94b99df4e9653654e14fffb6";

pub fn default_memory_storage() -> SerialEVMStorage {
    evm_storage::memory_storage(
        DEFAULT_CONTRACT_ADDRESS, 
        CONTRACT_BYTECODE, 
        ADMIN_ADDRESS)
}

pub fn concurrent_evm_storage() -> ConcurrentEVMStorage {
    evm_storage::concurrent_evm_storage(
        DEFAULT_CONTRACT_ADDRESS, 
        CONTRACT_BYTECODE, 
        ADMIN_ADDRESS)
}