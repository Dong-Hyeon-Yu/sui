use ethers_providers::{Provider, MockProvider};
use narwhal_types::BatchDigest;
use sslab_execution::{
    utils::{
        smallbank_contract_benchmark::concurrent_evm_storage, 
        test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID},
    }, 
    types::ExecutableEthereumBatch};
use tokio::time::Instant;



use crate::{SimulationResult, AddressBasedConflictGraph, nezha_core::ConcurrencyLevelManager};

fn get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn get_nezha_executor() -> ConcurrencyLevelManager {
    ConcurrencyLevelManager::new(concurrent_evm_storage(), 10)
}

/* this test is for debuging nezha algorithm under a smallbank workload */
#[tokio::test]
async fn test_smallbank() {
    let nezha = Box::pin(get_nezha_executor());
    let handler = get_smallbank_handler();

    //given
    let skewness = 0.6;
    let batch_size = 200;
    let block_concurrency = 30;
    let mut consensus_output = Vec::new();
    for _ in 0..block_concurrency {
        let mut tmp = Vec::new();
        for _ in 0..batch_size {
            tmp.push(handler.random_operation(skewness, 10_000))
        }
        consensus_output.push(ExecutableEthereumBatch::new(tmp, BatchDigest::default()));
    }

    //when
    let total = Instant::now();
    let mut now = Instant::now();
    let SimulationResult { rw_sets, .. } = nezha.simulate(consensus_output).await;
    let mut time = now.elapsed().as_millis();
    println!("Simulation took {} ms for {} transactions.", time, rw_sets.len());


    now = Instant::now();
    let scheduled_info = AddressBasedConflictGraph::construct(rw_sets)
        .hierarchcial_sort()
        .reorder()
        .extract_schedule();
    time = now.elapsed().as_millis();
    println!("Scheduling took {} ms.", time);

    let scheduled_tx_len = scheduled_info.scheduled_txs_len();
    let aborted_tx_len =  scheduled_info.aborted_txs_len();

    now = Instant::now();
    nezha._concurrent_commit(scheduled_info, 1).await;
    time = now.elapsed().as_millis();

    println!("Concurrent commit took {} ms for {} transactions.", time, scheduled_tx_len);
    println!("Abort rate: {:.2} ({}/{} aborted)", (aborted_tx_len as f64) * 100.0 / (scheduled_tx_len+aborted_tx_len) as f64, aborted_tx_len, scheduled_tx_len+aborted_tx_len);
    println!("Total time: {} ms", total.elapsed().as_millis());
}


/* this test is for debuging nezha algorithm under a smallbank workload */
#[tokio::test]
async fn test_par_smallbank() {
    let nezha = Box::pin(get_nezha_executor());
    let handler = get_smallbank_handler();

    //given
    let skewness = 0.6;
    let batch_size = 200;
    let block_concurrency = 30;
    let mut consensus_output = Vec::new();
    for _ in 0..block_concurrency {
        let mut tmp = Vec::new();
        for _ in 0..batch_size {
            tmp.push(handler.random_operation(skewness, 10_000))
        }
        consensus_output.push(ExecutableEthereumBatch::new(tmp, BatchDigest::default()));
    }

    //when
    let total = Instant::now();
    let mut now = Instant::now();
    let SimulationResult { rw_sets, .. } = nezha.simulate(consensus_output).await;
    let mut time = now.elapsed().as_millis();
    println!("Simulation took {} ms for {} transactions.", time, rw_sets.len());


    now = Instant::now();
    let (scheduled_info, _) = AddressBasedConflictGraph::par_construct(rw_sets).await
            .hierarchcial_sort()
            .reorder()
            .par_extract_schedule().await;
    time = now.elapsed().as_millis();
    println!("Scheduling took {} ms.", time);

    let scheduled_tx_len = scheduled_info.scheduled_txs_len();
    let aborted_tx_len =  scheduled_info.aborted_txs_len();

    now = Instant::now();
    nezha._concurrent_commit(scheduled_info, 1).await;
    time = now.elapsed().as_millis();

    println!("Concurrent commit took {} ms for {} transactions.", time, scheduled_tx_len);
    println!("Abort rate: {:.2} ({}/{} aborted)", (aborted_tx_len as f64) * 100.0 / (scheduled_tx_len+aborted_tx_len) as f64, aborted_tx_len, scheduled_tx_len+aborted_tx_len);
    println!("Total time: {} ms", total.elapsed().as_millis());
}
