use std::collections::BTreeMap;

use ethers_core::types::{H160, U64};
use evm::{
    backend::Backend, 
    executor::stack::{
        PrecompileFn, StackExecutor, MemoryStackState, StackSubstateMetadata
    }
};

use crate::types::{ChainConfig, SpecId};

use super::backend::{ExecutionBackend, ExecutionResult, ApplyBackend};

#[derive(Clone, Debug)]
pub struct EvmStorage<B: Backend+ApplyBackend+Clone+Default> {
    backend: B,  
    precompiles: BTreeMap<H160, PrecompileFn>,
    config: ChainConfig,
}

impl<B: Backend+ApplyBackend+Clone+Default> EvmStorage<B> {
    pub fn new(
        chain_id: U64, 
        backend: B, 
        precompiles: BTreeMap<H160, PrecompileFn>
    ) -> Self {

        let config = ChainConfig::new(SpecId::try_from_u8(chain_id.byte(0)).unwrap());

        Self { 
            backend,
            precompiles,
            config
        }
    }

    pub fn executor(
        &self, 
        gas_limit: u64, 
        simulation: bool
    ) -> StackExecutor<MemoryStackState<B>, BTreeMap<H160, PrecompileFn>> {

        StackExecutor::new_with_precompiles(
            MemoryStackState::new(StackSubstateMetadata::new(gas_limit, self.config()), &self.backend),
            self.config(),
            self.precompiles(),
            simulation
        )
    }

    pub fn snapshot(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            precompiles: self.precompiles.clone(),
            config: self.config.clone(),
        }
    }


    pub fn get_storage(&self) -> &B {
        &self.backend
    }

    pub fn as_ref(&self) -> &Self {
        self
    }
}

impl<B: Backend+ApplyBackend+Clone+Default> Default for EvmStorage<B> {
    fn default() -> Self {
        EvmStorage::new(
            ethers_core::types::U64::from(9), 
            B::default(),
            BTreeMap::new(),
        )
    }
}

impl<B: Backend+ApplyBackend+Clone+Default> ExecutionBackend for EvmStorage<B> {
    fn config(&self) -> &evm::Config {
        self.config.config()
    }

    fn precompiles(&self) -> &std::collections::BTreeMap<H160, evm::executor::stack::PrecompileFn> {
        &self.precompiles
    }

    fn code(&self, address: ethers_core::types::Address) -> Vec<u8> {
        self.backend.code(address)
    }

    fn apply_all_effects(&self, execution_result: &ExecutionResult) {
        let effects = execution_result.effects.clone();

        self.backend.apply(effects, false);
    }

    fn apply_local_effect(&self, effect: Vec<evm::backend::Apply>) {
        self.backend.apply(effect, false); 
    }
}