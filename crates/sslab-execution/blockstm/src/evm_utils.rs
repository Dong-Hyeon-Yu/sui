use std::collections::BTreeMap;

use ethers::types::{H160, U64};
use evm::{
    backend::{Backend, Apply, Log}, 
    executor::stack::{
        PrecompileFn, MultiversionStackExecutor, MemoryStackState, StackSubstateMetadata, RwSet
    }, ExitError, ExitReason
};
use sui_types::error::SuiError;
use tracing::{debug, warn};
use crate::executor::EtherMVHashMapView;
use sslab_execution::types::{ChainConfig, SpecId, EthereumTransaction};
use sslab_execution::evm_storage::backend::{ExecutionBackend, ExecutionResult, ApplyBackend};

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

    pub fn executor<'a>(
        &'a self, 
        gas_limit: u64, 
        multiversion_view: &'a EtherMVHashMapView,
    ) -> MultiversionStackExecutor<MemoryStackState<B>, BTreeMap<H160, PrecompileFn>, EtherMVHashMapView> {

        MultiversionStackExecutor::new_with_precompiles(
            MemoryStackState::new(StackSubstateMetadata::new(gas_limit, self.config()), &self.backend),
            self.config(),
            self.precompiles(),
            &multiversion_view
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
            U64::from(9), 
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

    fn code(&self, address: ethers::types::Address) -> Vec<u8> {
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

pub fn execute_tx<'a, B>(
    tx: &EthereumTransaction, 
    snapshot: &EvmStorage<B>,
    versioned_map: &EtherMVHashMapView
) -> Result<Option<(Vec<Apply>, Vec<Log>, RwSet)>, SuiError> 
where
    B: Backend + ApplyBackend + Default + Clone
{
    let mut executor = snapshot.executor(tx.gas_limit(), versioned_map);

    let mut effect: Vec<Apply> = vec![];
    let mut log: Vec<Log> = vec![];

    if let Some(to_addr) = tx.to_addr() {

        let (reason, _) = & executor.transact_call(
            tx.caller(), *to_addr, tx.value(), tx.data().unwrap().to_owned().to_vec(), 
            tx.gas_limit(), tx.access_list()
        );

        match process_transact_call_result(reason) {
            Ok(fail) => {
                if fail {
                    return Ok(None);
                } else {
                    // debug!("success to execute a transaction {}", tx.id());
                    let rw_set = executor.rw_set().clone();
                    (effect, log) = executor.into_state().deconstruct();
                    return Ok(Some((effect, log, rw_set)));
                }
            },
            Err(e) => return Err(e)
        }
    } else { 
        if let Some(data) = tx.data() {
             // create EOA
            let init_code = data.to_vec();
            let (reason, _) = &executor.transact_create(tx.caller(), tx.value(), init_code.clone(), tx.gas_limit(), tx.access_list());

            match process_transact_create_result(reason) {
                Ok(fail) => {
                    if fail {
                        return Ok(None);
                    } else {
                        debug!("success to deploy a contract!");
                        let rw_set = executor.rw_set().clone();
                        (effect, log) = executor.into_state().deconstruct();
                        return Ok(Some((effect, log, rw_set)));
                    }
                },
                Err(e) => return Err(e)
                
            }
        } else {
            // create user account
            debug!("create user account: {:?} with balance {:?} and nonce {:?}", tx.caller(), tx.value(), tx.nonce());
            effect.push(Apply::Modify {
                address: tx.caller(),
                basic: evm::backend::Basic { balance: tx.value(), nonce: tx.nonce() },
                code: None,
                storage: BTreeMap::new(),
                reset_storage: false,
            });
            log.push(Log {
                address: tx.caller(),
                topics: vec![],
                data: vec![],
            });
            // Self::_process_local_effect(store, effect, log, &mut effects, &mut logs);
            return Ok(Some((effect, log, RwSet::new())));
        }
    }
}

fn process_transact_call_result(reason: &ExitReason) -> Result<bool, SuiError> {
    match reason {
        ExitReason::Succeed(_) => {
            Ok(false)
        }
        ExitReason::Revert(e) => {
            // do nothing: explicit revert is not an error
            debug!("tx execution revert: {:?}", e);
            Ok(true)
        }
        ExitReason::Error(e) => {
            // do nothing: normal EVM error
            match e {
                ExitError::NotEstimatedYet => {
                    Err(SuiError::ExecutionError(String::from("ESTIMATE")))
                },
                _ => {
                    warn!("tx execution error: {:?}", e);
                    Ok(true)
                }
            }
        }
        ExitReason::Fatal(e) => {
            warn!("tx execution fatal error: {:?}", e);
            Err(SuiError::ExecutionError(String::from("Fatal error occurred on EVM!")))
        }
    }
}

fn process_transact_create_result(reason: &ExitReason) -> Result<bool, SuiError> {
    match reason {
        ExitReason::Succeed(_) => {
            Ok(false)
        }
        ExitReason::Revert(e) => {
            // do nothing: explicit revert is not an error
            debug!("fail to deploy contract: {:?}", e);
            Ok(true)
        }
        ExitReason::Error(e) => {
            // do nothing: normal EVM error
            match e {
                ExitError::NotEstimatedYet => {
                    Err(SuiError::ExecutionError(String::from("ESTIMATE")))
                },
                _ => {
                    warn!("fail to deploy contract: {:?}", e);
                    Ok(true)
                }
            }
        }
        ExitReason::Fatal(e) => {
            warn!("fatal error occurred when deploying contract: {:?}", e);
            Err(SuiError::ExecutionError(String::from("Fatal error occurred on EVM!")))
        }
    }
}