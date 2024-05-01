use ethers_providers::{MockProvider, Provider};
use reth::primitives::TransactionSignedEcRecovered;
use sslab_execution::{
    types::ExecutableEthereumBatch,
    utils::{
        smallbank_contract_benchmark::concurrent_memory_database,
        test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID},
    },
};
use tokio::time::Instant;

use crate::{nezha_core::ConcurrencyLevelManager, KeyBasedDependencyGraph, SimulationResultV2};

fn _get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn get_nezha_executor() -> ConcurrencyLevelManager {
    ConcurrencyLevelManager::new(concurrent_memory_database(), 10)
}

fn _create_random_smallbank_workload(
    skewness: f32,
    batch_size: usize,
    block_concurrency: usize,
) -> Vec<ExecutableEthereumBatch<TransactionSignedEcRecovered>> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, 100_000)
}

/* this test is for debuging nezha algorithm under a smallbank workload */
#[tokio::test]
async fn test_smallbank() {
    let nezha = Box::pin(get_nezha_executor());

    //given
    let skewness = 0.6;
    let batch_size = 200;
    let block_concurrency = 30;
    let consensus_output =
        _create_random_smallbank_workload(skewness, batch_size, block_concurrency);

    //when
    let total = Instant::now();
    let mut now = Instant::now();
    let SimulationResultV2 { rw_sets, .. } = nezha.simulate(consensus_output).await;
    let mut time = now.elapsed().as_millis();
    println!(
        "Simulation took {} ms for {} transactions.",
        time,
        rw_sets.len()
    );

    now = Instant::now();
    let scheduled_info = KeyBasedDependencyGraph::construct(rw_sets)
        .hierarchcial_sort()
        .reorder()
        .extract_schedule();
    time = now.elapsed().as_millis();
    println!("Scheduling took {} ms.", time);

    let scheduled_tx_len = scheduled_info.scheduled_txs_len();
    let aborted_tx_len = scheduled_info.aborted_txs_len();

    now = Instant::now();
    nezha._concurrent_commit(scheduled_info.scheduled_txs).await;
    time = now.elapsed().as_millis();

    println!(
        "Concurrent commit took {} ms for {} transactions.",
        time, scheduled_tx_len
    );
    println!(
        "Abort rate: {:.2} ({}/{} aborted)",
        (aborted_tx_len as f64) * 100.0 / (scheduled_tx_len + aborted_tx_len) as f64,
        aborted_tx_len,
        scheduled_tx_len + aborted_tx_len
    );
    println!("Total time: {} ms", total.elapsed().as_millis());
}

/* this test is for debuging nezha algorithm under a smallbank workload */
#[tokio::test]
async fn test_par_smallbank() {
    let nezha = Box::pin(get_nezha_executor());

    //given
    let skewness = 0.6;
    let batch_size = 200;
    let block_concurrency = 30;
    let consensus_output =
        _create_random_smallbank_workload(skewness, batch_size, block_concurrency);

    //when
    let total = Instant::now();
    let mut now = Instant::now();
    let SimulationResultV2 { rw_sets, .. } = nezha.simulate(consensus_output).await;
    let mut time = now.elapsed().as_millis();
    println!(
        "Simulation took {} ms for {} transactions.",
        time,
        rw_sets.len()
    );

    now = Instant::now();
    let scheduled_info = KeyBasedDependencyGraph::par_construct(rw_sets)
        .await
        .hierarchcial_sort()
        .reorder()
        .par_extract_schedule()
        .await;
    time = now.elapsed().as_millis();
    println!("Scheduling took {} ms.", time);

    let scheduled_tx_len = scheduled_info.scheduled_txs_len();
    let aborted_tx_len = scheduled_info.aborted_txs_len();

    now = Instant::now();
    nezha._concurrent_commit(scheduled_info.scheduled_txs).await;
    time = now.elapsed().as_millis();

    println!(
        "Concurrent commit took {} ms for {} transactions.",
        time, scheduled_tx_len
    );
    println!(
        "Abort rate: {:.2} ({}/{} aborted)",
        (aborted_tx_len as f64) * 100.0 / (scheduled_tx_len + aborted_tx_len) as f64,
        aborted_tx_len,
        scheduled_tx_len + aborted_tx_len
    );
    println!("Total time: {} ms", total.elapsed().as_millis());
}

/* this test is for debuging nezha algorithm under a smallbank workload */
#[tokio::test]
async fn test_par_smallbank_for_advanced_nezha() {
    let nezha = Box::pin(get_nezha_executor());

    //given
    let skewness = 0.6;
    let batch_size = 200;
    let block_concurrency = 30;
    let consensus_output =
        _create_random_smallbank_workload(skewness, batch_size, block_concurrency);

    //when
    let now = Instant::now();
    let _ = nezha._execute(consensus_output).await;
    let time = now.elapsed().as_millis();
    println!("execution took {} ms", time);
}
