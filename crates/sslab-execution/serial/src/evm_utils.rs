use std::collections::BTreeMap;
use sui_types::error::SuiError;
use evm::backend::{Apply, Log, Backend};
use sslab_execution::{
    types::EthereumTransaction, 
    evm_storage::{EvmStorage, backend::ApplyBackend}, 
    executor::EvmExecutionUtils
};
use tracing::debug;


pub fn execute_tx<B>(
    tx: &EthereumTransaction, 
    snapshot: &EvmStorage<B>
) -> Result<Option<(Vec<Apply>, Vec<Log>)>, SuiError> 
where
    B: Backend + ApplyBackend + Default + Clone
{
    let mut executor = snapshot.executor(tx.gas_limit(), false);

    let mut effect: Vec<Apply> = vec![];
    let mut log: Vec<Log> = vec![];

    if let Some(to_addr) = tx.to_addr() {

        let (reason, _) = & executor.transact_call(
            tx.caller(), *to_addr, tx.value(), tx.data().unwrap().to_owned().to_vec(), 
            tx.gas_limit(), tx.access_list()
        );

        match EvmExecutionUtils::process_transact_call_result(reason) {
            Ok(fail) => {
                if fail {
                    return Ok(None);
                } else {
                    // debug!("success to execute a transaction {}", tx.id());
                    (effect, log) = executor.into_state().deconstruct();
                    return Ok(Some((effect, log)));
                }
            },
            Err(e) => return Err(e)
        }
    } else { 
        if let Some(data) = tx.data() {
             // create EOA
            let init_code = data.to_vec();
            let (reason, _) = &executor.transact_create(tx.caller(), tx.value(), init_code.clone(), tx.gas_limit(), tx.access_list());

            match EvmExecutionUtils::process_transact_create_result(reason) {
                Ok(fail) => {
                    if fail {
                        return Ok(None);
                    } else {
                        debug!("success to deploy a contract!");

                        (effect, log) = executor.into_state().deconstruct();
                        return Ok(Some((effect, log)));
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
            return Ok(Some((effect, log)));
        }
    }
}