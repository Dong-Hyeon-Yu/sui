use std::collections::BTreeMap;

use ethers::types::U64;
use sslab_execution::{utils::smallbank_contract_benchmark::{
    DEFAULT_CONTRACT_ADDRESS, 
    CONTRACT_BYTECODE, 
    ADMIN_ADDRESS
}, evm_storage::backend::CMemoryBackend};

use crate::evm_utils::EvmStorage;

pub fn concurrent_evm_storage() -> EvmStorage<CMemoryBackend> {
    let memory_backend = sslab_execution::evm_storage::cmemory_backend(
        DEFAULT_CONTRACT_ADDRESS, 
        CONTRACT_BYTECODE, 
        ADMIN_ADDRESS);
    EvmStorage::new(
        U64::from(9),
        memory_backend,
        BTreeMap::new(),
    )
}