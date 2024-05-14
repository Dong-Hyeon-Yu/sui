pub mod smallbank_contract_benchmark;

pub mod test_utils;

pub mod in_memory_db_utils {
    use std::str::FromStr as _;

    use reth::primitives::{Address, Bytes};
    use reth::revm::{interpreter::analysis::to_analysed, InMemoryDB};

    use crate::db::in_memory_db::InMemoryConcurrentDB;

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
            to_analysed(reth_bytecode),
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
            to_analysed(reth_bytecode),
        );

        db.insert_account_info(
            Address::from_str(contract_addr).unwrap(),
            smallbank_contract,
        );
        db
    }
}
