use std::sync::Arc;

use itertools::Itertools as _;
use reth::primitives::Block;
use sslab_execution::{
    get_provider_factory,
    traits::Executable,
    types::ExecutableEthereumBatch,
    utils::{
        smallbank_contract_benchmark::{
            cache_state_with_smallbank_contract, get_smallbank_handler,
        },
        test_utils::{convert_into_block, default_chain_spec},
    },
};
use tokio::time::Instant;

use crate::{nezha_core::OptME, types::IndexedEthereumTransaction, KeyBasedDependencyGraph};

fn get_executor() -> OptME {
    let chain_spec = Arc::new(default_chain_spec());
    OptME::new_with_db(
        get_provider_factory(chain_spec.clone()),
        Some(cache_state_with_smallbank_contract()),
        chain_spec,
    )
}

fn _create_random_smallbank_workload(
    skewness: f32,
    batch_size: usize,
    block_concurrency: usize,
) -> Vec<ExecutableEthereumBatch> {
    let handler = get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, 100_000)
}

/* this test is for debuging nezha algorithm under a smallbank workload */
#[tokio::test]
async fn test_smallbank() {
    let nezha = get_executor();

    //given
    let skewness = 0.6;
    let batch_size = 200;
    let block_concurrency = 30;
    let consensus_output = convert_into_block(_create_random_smallbank_workload(
        skewness,
        batch_size,
        block_concurrency,
    ));

    let (block, senders) = consensus_output.into_components();
    let Block { header, body, .. } = block;
    let tx_list = body
        .into_iter()
        .zip(senders)
        .enumerate()
        .map(|(id, (tx, signer))| IndexedEthereumTransaction {
            tx,
            signer,
            id: id as u64,
        })
        .collect_vec();

    //when
    let total = Instant::now();
    let mut now = Instant::now();

    let rw_sets = nezha._simulate(&header, tx_list).await.unwrap();
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

    drop(nezha);
}

/* this test is for debuging nezha algorithm under a smallbank workload */
#[tokio::test]
async fn test_par_smallbank() {
    let nezha = get_executor();

    //given
    let skewness = 0.6;
    let batch_size = 200;
    let block_concurrency = 30;
    let consensus_output = convert_into_block(_create_random_smallbank_workload(
        skewness,
        batch_size,
        block_concurrency,
    ));

    let (block, senders) = consensus_output.into_components();
    let Block { header, body, .. } = block;
    let tx_list = body
        .into_iter()
        .zip(senders)
        .enumerate()
        .map(|(id, (tx, signer))| IndexedEthereumTransaction {
            tx,
            signer,
            id: id as u64,
        })
        .collect_vec();

    //when
    let total = Instant::now();
    let mut now = Instant::now();
    let rw_sets = nezha._simulate(&header, tx_list).await.unwrap();
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

    drop(nezha);
}

/* this test is for debuging nezha algorithm under a smallbank workload */
#[tokio::test]
async fn test_par_smallbank_for_advanced_nezha() {
    let mut nezha = get_executor();

    //given
    let skewness = 0.6;
    let batch_size = 200;
    let block_concurrency = 30;
    let consensus_output = convert_into_block(_create_random_smallbank_workload(
        skewness,
        batch_size,
        block_concurrency,
    ));

    //when
    let now = Instant::now();
    let _ = nezha._execute_inner(consensus_output).await;
    let time = now.elapsed().as_millis();
    println!("execution took {} ms", time);

    drop(nezha);
}
