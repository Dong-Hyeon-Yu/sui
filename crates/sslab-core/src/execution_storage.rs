use std::collections::{BTreeMap, HashSet};
use ethers_core::types::{U64, H160};
use evm::{backend::{MemoryBackend, Apply, Log, ApplyBackend}, executor::stack::{PrecompileFn, StackExecutor, MemoryStackState, StackSubstateMetadata}, Config};
use sui_types::{error::SuiResult, digests::TransactionDigest};

use crate::{types::{SpecId, ChainConfig}, transaction_manager::MIN_HASHMAP_CAPACITY};

const DEFAULT_TX_GAS_LIMIT: u64 = 21000;

pub struct ExecutionResult {
    pub logs: Vec<Log>,
    pub effects: Vec::<Apply>,
}

#[async_trait::async_trait]
pub trait ExecutionBackend {

    fn config(&self) -> &Config;

    fn precompiles(&self) -> &BTreeMap<H160, PrecompileFn>;

    fn backend(&self) -> &MemoryBackend<'static>;

    fn apply_all_effects(&mut self, cert: &TransactionDigest, execution_result: &ExecutionResult);

    fn is_tx_already_executed(&self, tx_digest: &TransactionDigest) -> SuiResult<bool>;
}

/// This storage is used for evm global state.
pub struct MemoryStorage {
    executed_tx: HashSet<TransactionDigest>,
    backend: MemoryBackend<'static>,  //TODO: change to MutexTable for concurrent execution.
    precompiles: BTreeMap<H160, PrecompileFn>,
    config: ChainConfig,
    // checkpoint:  ArcSwap<BTreeMap<H160, MemoryAccount>>?
    // mutex_table: MutexTable<TransactionDigest>, // TODO MutexTable for transaction locks (prevent concurrent execution of same transaction)
}

impl MemoryStorage {
    pub fn new(chain_id: U64, backend: MemoryBackend<'static>, precompiles: BTreeMap<H160, PrecompileFn>) -> Self {
        // let backend = Arc::new(MemoryBackend::new(vicinity, BTreeMap::new()));
        let config = ChainConfig::new(SpecId::try_from_u8(chain_id.byte(0)).unwrap());
        Self { 
            executed_tx: HashSet::with_capacity(MIN_HASHMAP_CAPACITY), 
            backend: backend, 
            precompiles, 
            config }
    }

    pub fn executor(&self) -> StackExecutor<MemoryStackState<MemoryBackend>, BTreeMap<H160, PrecompileFn>> {
        StackExecutor::new_with_precompiles(
            MemoryStackState::new(StackSubstateMetadata::new(DEFAULT_TX_GAS_LIMIT, self.config()), self.backend()),
            self.config.config(),
            self.precompiles(),
        )
    }
}

#[async_trait::async_trait]
impl ExecutionBackend for MemoryStorage {

    fn config(&self) -> &Config {
        self.config.config()
    }

    fn precompiles(&self) -> &BTreeMap<H160, PrecompileFn> {
        &self.precompiles
    }

    fn backend(&self) -> &MemoryBackend<'static> {
        &self.backend
    }

    fn apply_all_effects(&mut self, cert: &TransactionDigest, execution_result: &ExecutionResult) {

        let effects = execution_result.effects.clone();
        let logs = execution_result.logs.clone();
        self.backend.apply(effects, logs, false);
        
        self.executed_tx.insert(*cert);
    }
     
    fn is_tx_already_executed(&self, tx_digest: &TransactionDigest) -> SuiResult<bool> {
        Ok(self.executed_tx.contains(tx_digest))
    }
}
