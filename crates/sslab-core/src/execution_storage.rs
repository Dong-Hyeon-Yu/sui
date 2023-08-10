use std::collections::{BTreeMap, HashSet};
use ethers_core::types::{U64, H160, U256, Address};
use evm::{backend::{MemoryBackend, Apply, Log, ApplyBackend, Backend}, executor::stack::{PrecompileFn, StackExecutor, MemoryStackState, StackSubstateMetadata}, Config};
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

    fn code(&self, address: Address) -> Vec<u8>;

    fn apply_all_effects(&mut self, cert: &TransactionDigest, execution_result: &ExecutionResult);

    fn is_tx_already_executed(&self, tx_digest: &TransactionDigest) -> SuiResult<bool>;
}

/// This storage is used for evm global state.
#[derive(Clone)]
pub struct MemoryStorage {
    executed_tx: HashSet<TransactionDigest>,
    backend: MemoryBackend,  //TODO: change to MutexTable for concurrent execution.
    precompiles: BTreeMap<H160, PrecompileFn>,
    config: ChainConfig,
    // checkpoint:  ArcSwap<BTreeMap<H160, MemoryAccount>>?
    // mutex_table: MutexTable<TransactionDigest>, // TODO MutexTable for transaction locks (prevent concurrent execution of same transaction)
}

impl MemoryStorage {
    pub fn new(chain_id: U64, backend: MemoryBackend, precompiles: BTreeMap<H160, PrecompileFn>) -> Self {
        
        let config = ChainConfig::new(SpecId::try_from_u8(chain_id.byte(0)).unwrap());

        Self { 
            executed_tx: HashSet::with_capacity(MIN_HASHMAP_CAPACITY),
            backend,
            precompiles,
            config
        }
    }

    pub fn default(chain_id: SpecId) -> Self {
        let vicinity = evm::backend::MemoryVicinity { 
            gas_price: U256::zero(), 
            origin: H160::default(), 
            chain_id: U256::from(chain_id as u64), 
            block_hashes: Vec::new(), 
            block_number: U256::zero(), 
            block_coinbase: H160::default(), 
            block_timestamp: U256::from(0), 
            block_difficulty: U256::from(1), 
            block_gas_limit: U256::from(30_000_000), 
            block_base_fee_per_gas: U256::zero(), //Gwei 
            block_randomness: None
        };

        MemoryStorage::new(
            ethers_core::types::U64::from(chain_id as u64), 
            MemoryBackend::new(vicinity, BTreeMap::new()),
            BTreeMap::new(),
        )
    }

    pub fn executor(&self) -> StackExecutor<MemoryStackState<MemoryBackend>, BTreeMap<H160, PrecompileFn>> {

        StackExecutor::new_with_precompiles(
            MemoryStackState::new(StackSubstateMetadata::new(DEFAULT_TX_GAS_LIMIT, self.config()), &self.backend),
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

    fn code(&self, address: Address) -> Vec<u8> {
        self.backend.code(address)
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
