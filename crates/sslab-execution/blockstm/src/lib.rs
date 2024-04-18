// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0
pub mod errors;
mod evm_utils;
pub mod executor;
pub mod infallible;
pub mod mvhashmap;
mod outcome_array;
#[cfg(any(test, feature = "fuzzing"))]
pub mod proptest_types;
mod scheduler;
pub mod task;
mod txn_last_input_output;
#[cfg(test)]
mod unit_tests;
pub mod utils;

use evm::{backend::Apply, executor::stack::RwSet};
use evm_utils::execute_tx;
use executor::EtherMVHashMapView;
use sslab_execution::{
    evm_storage::backend::{CMemoryBackend, ExecutionBackend},
    executor::Executable,
    types::EthereumTransaction,
};
use std::sync::Arc;
use sui_types::error::SuiError;
use task::ExecutorTask;
use tracing::{debug, warn};

use crate::executor::ParallelTransactionExecutor;

struct EtherTxn(EthereumTransaction);

impl task::Transaction for EtherTxn {
    type Key = ethers::types::H256;
    type Value = ethers::types::H256;
}

struct EtherTxnOutput(Vec<Apply>, RwSet);

impl task::TransactionOutput for EtherTxnOutput {
    type T = EtherTxn;

    fn get_writes(
        &self,
    ) -> Vec<(
        <Self::T as task::Transaction>::Key,
        <Self::T as task::Transaction>::Value,
    )> {
        self.1
            .writes()
            .into_iter()
            .flat_map(|(_, item)| item.clone().into_iter().collect::<Vec<_>>())
            .collect::<Vec<_>>()
    }

    fn skip_output() -> Self {
        EtherTxnOutput(vec![], RwSet::default())
    }
}

struct EvmExecutorTask {
    global_state: Arc<evm_utils::EvmStorage<CMemoryBackend>>,
}

impl ExecutorTask for EvmExecutorTask {
    type T = EtherTxn;
    type Output = EtherTxnOutput;
    type Error = SuiError;
    type Argument = Arc<evm_utils::EvmStorage<CMemoryBackend>>;

    fn init(args: Self::Argument) -> Self {
        //TODO: modify to get the referce of Arc<EvmStorage> ?, clone() is expensive.
        Self { global_state: args }
    }

    fn execute_transaction(
        &self,
        view: &executor::MVHashMapView<
            <Self::T as task::Transaction>::Key,
            <Self::T as task::Transaction>::Value,
        >,
        txn: &Self::T,
    ) -> task::ExecutionStatus<Self::Output, Self::Error> {
        let ether_versioned_view = EtherMVHashMapView {
            versioned_map: view,
        };
        match execute_tx(&txn.0, self.global_state.as_ref(), &ether_versioned_view) {
            Ok(Some((effects, _, rw_set))) => {
                task::ExecutionStatus::Success(EtherTxnOutput(effects, rw_set))
            }
            Ok(None) => task::ExecutionStatus::Success(EtherTxnOutput(vec![], RwSet::default())),
            Err(e) => {
                debug!("Error executing transaction: {:?}", e);
                task::ExecutionStatus::Abort(e)
            }
        }
    }
}

pub struct BlockSTM {
    global_state: Arc<evm_utils::EvmStorage<CMemoryBackend>>,
}

impl BlockSTM {
    pub fn new(global_state: Arc<evm_utils::EvmStorage<CMemoryBackend>>) -> Self {
        Self { global_state }
    }
}

#[async_trait::async_trait]
impl Executable for BlockSTM {
    async fn execute(
        &self,
        consensus_output: Vec<sslab_execution::types::ExecutableEthereumBatch>,
    ) {
        let executor: ParallelTransactionExecutor<EtherTxn, EvmExecutorTask> =
            ParallelTransactionExecutor::new();

        for batch in consensus_output.into_iter() {
            let txn_to_execute = batch
                .data()
                .clone()
                .into_iter()
                .map(|txn| EtherTxn(txn))
                .collect();

            match executor.execute_transactions_parallel(self.global_state.clone(), txn_to_execute)
            {
                Ok(effects) => {
                    let _effects = effects.into_iter().flat_map(|output| output.0).collect();
                    self.global_state.apply_local_effect(_effects);
                }
                Err(e) => {
                    warn!("Error executing transaction: {:?}", e);
                }
            }
        }
    }
}

#[cfg(feature = "latency")]
impl BlockSTM {
    pub async fn execute_and_return_commit_latency(
        &self,
        consensus_output: Vec<sslab_execution::types::ExecutableEthereumBatch>,
    ) -> u128 {
        let executor: ParallelTransactionExecutor<EtherTxn, EvmExecutorTask> =
            ParallelTransactionExecutor::new();
        let mut commit_latency = 0;

        for batch in consensus_output.into_iter() {
            let txn_to_execute = batch
                .data()
                .clone()
                .into_iter()
                .map(|txn| EtherTxn(txn))
                .collect();
            match executor.execute_transactions_parallel(self.global_state.clone(), txn_to_execute)
            {
                Ok(effects) => {
                    let _effects = effects.into_iter().flat_map(|output| output.0).collect();
                    let latency = std::time::Instant::now();
                    self.global_state.apply_local_effect(_effects);
                    commit_latency += latency.elapsed().as_millis();
                }
                Err(e) => {
                    warn!("Error executing transaction: {:?}", e);
                }
            }
        }

        commit_latency
    }
}
