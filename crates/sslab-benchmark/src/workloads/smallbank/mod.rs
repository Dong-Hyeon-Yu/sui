mod protos;
mod smallbank_contract;
pub mod smallbank_transaction_handler;
// use crate::workloads::smallbank::contract::small_bank::__BYTECODE;

mod contract {
    pub use super::smallbank_contract::*;
}