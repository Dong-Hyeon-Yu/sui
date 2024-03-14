use ethers_core::types::H160;
use evm::{
    backend::{Apply, Log},
    executor::stack::PrecompileFn,
    Config,
};
use std::collections::BTreeMap;

mod memory_backend;

pub use memory_backend::MemoryBackend;

pub struct ExecutionResult {
    pub logs: Vec<Log>,
    pub effects: Vec<Apply>,
}

pub trait ApplyBackend {
    fn apply(&mut self, values: Vec<Apply>, delete_empty: bool);
}

pub trait ExecutionBackend {
    fn config(&self) -> &Config;

    fn precompiles(&self) -> &BTreeMap<H160, PrecompileFn>;

    fn code(&self, address: H160) -> Vec<u8>;

    fn apply_all_effects(&mut self, execution_result: &ExecutionResult);

    fn apply_local_effect(&mut self, effect: Vec<Apply>);
}
