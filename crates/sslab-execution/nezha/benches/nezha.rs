#![allow(dead_code)]
use std::sync::Arc;

use parking_lot::RwLock;

use criterion::Throughput;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use ethers_providers::{MockProvider, Provider};

use sslab_execution::{
    types::ExecutableEthereumBatch,
    utils::smallbank_contract_benchmark::concurrent_evm_storage,
    utils::test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID},
};

use sslab_execution_nezha::{
    AddressBasedConflictGraph, ConcurrencyLevelManager, SimulatedTransaction, SimulationResult,
};

const DEFAULT_BATCH_SIZE: usize = 200;
const DEFAULT_BLOCK_CONCURRENCY: usize = 12;
const DEFAULT_SKEWNESS: f32 = 0.0;

fn _get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn _get_nezha_executor(clevel: usize) -> ConcurrencyLevelManager {
    ConcurrencyLevelManager::new(concurrent_evm_storage(), clevel)
}

fn _create_random_smallbank_workload(
    skewness: f32,
    batch_size: usize,
    block_concurrency: usize,
) -> Vec<ExecutableEthereumBatch> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, 100_000)
}

fn _get_rw_sets(
    nezha: std::sync::Arc<ConcurrencyLevelManager>,
    consensus_output: Vec<ExecutableEthereumBatch>,
) -> Vec<SimulatedTransaction> {
    let (tx, rx) = std::sync::mpsc::channel();
    let _ = tokio::runtime::Handle::current().spawn(async move {
        let SimulationResult { rw_sets, .. } = nezha._simulate(consensus_output).await;
        tx.send(rw_sets).unwrap();
    });
    rx.recv().unwrap()
}

fn effective_ktps(
    effective_tps: Arc<RwLock<Vec<(usize, u128)>>>,
    batch_size: usize,
    num_of_batches: usize,
) -> f64 {
    let (mut committed, mut latency) = (0, 0);
    let len = effective_tps.read().len();
    if len == 0 {
        return 0.0;
    }
    for (a, b) in effective_tps.read().iter() {
        committed += a;
        latency += b;
    }
    let average_commtted_tx = committed as f64 / len as f64;
    let latency_in_ms = (latency / len as u128) as f64 / 1_000f64;
    let total = batch_size as f64 * num_of_batches as f64;
    let commit_rate = average_commtted_tx / total;
    let expected_number_of_trials = 1.0 / commit_rate;
    println!(
        "committed: {:?} / latency: {:?} (ms) / commit_rate: {:.4} / # of trials: {:.6}",
        average_commtted_tx, latency_in_ms, commit_rate, expected_number_of_trials
    );

    average_commtted_tx / (latency_in_ms * expected_number_of_trials)
}

fn blocksize(c: &mut Criterion) {
    let s = [0.0];
    let param = 1..81;
    let mut group = c.benchmark_group("Nezha");
    for zipfian in s {
        for i in param.clone() {
            group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));

            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "blocksize",
                    format!("zipfian: {}, block_concurrency: {}", zipfian, i),
                ),
                &i,
                |b, i| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || {
                                let consensus_output = _create_random_smallbank_workload(
                                    zipfian,
                                    DEFAULT_BATCH_SIZE,
                                    *i,
                                );
                                let nezha = _get_nezha_executor(*i);
                                (nezha, consensus_output)
                            },
                            |(nezha, consensus_output)| async move {
                                let latency = tokio::time::Instant::now();
                                let SimulationResult { rw_sets, .. } =
                                    nezha._simulate(consensus_output).await;
                                let scheduled_info = AddressBasedConflictGraph::construct(rw_sets)
                                    .hierarchcial_sort()
                                    .reorder()
                                    .extract_schedule();

                                let scheduled_txs_len = scheduled_info.scheduled_txs_len();

                                nezha._concurrent_commit(scheduled_info, 1).await;

                                effective_tps
                                    .write()
                                    .push((scheduled_txs_len, latency.elapsed().as_micros()));
                            },
                            BatchSize::SmallInput,
                        );
                },
            );

            if effective_tps.read().is_empty() {
                continue;
            }
            println!(
                "effective ktps: {:.2}",
                effective_ktps(effective_tps, DEFAULT_BATCH_SIZE, i)
            );
        }
    }
}

fn skewness(c: &mut Criterion) {
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 80..81;
    let mut group = c.benchmark_group("Nezha");
    for zipfian in s {
        for i in param.clone() {
            group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));

            let effective_tps = std::sync::Arc::new(RwLock::new(Vec::new()));

            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "skewness",
                    format!("zipfian: {}, block_concurrency: {}", zipfian, i),
                ),
                &(i, effective_tps.clone()),
                |b, (i, effective_tps)| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || {
                                let consensus_output = _create_random_smallbank_workload(
                                    zipfian,
                                    DEFAULT_BATCH_SIZE,
                                    *i,
                                );
                                let nezha = _get_nezha_executor(*i);
                                (nezha, consensus_output)
                            },
                            |(nezha, consensus_output)| async move {
                                let latency = tokio::time::Instant::now();
                                let SimulationResult { rw_sets, .. } =
                                    nezha._simulate(consensus_output).await;
                                let scheduled_info = AddressBasedConflictGraph::construct(rw_sets)
                                    .hierarchcial_sort()
                                    .reorder()
                                    .extract_schedule();
                                let scheduled_txs_len = scheduled_info.scheduled_txs_len();

                                nezha._concurrent_commit(scheduled_info, 1).await;

                                effective_tps
                                    .write()
                                    .push((scheduled_txs_len, latency.elapsed().as_micros()));
                            },
                            BatchSize::SmallInput,
                        );
                },
            );

            if effective_tps.read().is_empty() {
                continue;
            }
            println!(
                "effective ktps: {:.2}",
                effective_ktps(effective_tps, DEFAULT_BATCH_SIZE, i)
            );
        }
    }
}

fn parallelism_of_first_committer_wins_rule(c: &mut Criterion) {
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 80..81;
    let mut group = c.benchmark_group("Nezha");

    for i in param {
        for zipfian in s {
            group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));

            let parallelism_metrics = std::sync::Arc::new(RwLock::new(Vec::new()));

            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "parallelism",
                    format!("zipfian: {}, block_concurrency: {}", zipfian, i),
                ),
                &(i, parallelism_metrics.clone()),
                |b, (i, metrics)| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || {
                                let consensus_output = _create_random_smallbank_workload(
                                    zipfian,
                                    DEFAULT_BATCH_SIZE,
                                    *i,
                                );
                                let nezha = _get_nezha_executor(*i);
                                (nezha, consensus_output)
                            },
                            |(nezha, consensus_output)| async move {
                                metrics.write().push(
                                    nezha
                                        ._execute_and_return_parellism_metric(consensus_output)
                                        .await,
                                );
                            },
                            BatchSize::SmallInput,
                        );
                },
            );

            let (
                // mut total_tx,
                mut average_height,
                // mut std_height,
                // mut skewness_height,
                // mut max_height,
                mut depth,
            ) = (0 as f64, 0 as u32);
            let len = parallelism_metrics.read().len();
            for (_a1, a2, _a3, _a4, _a5, a6) in parallelism_metrics.read().iter() {
                average_height += a2;
                depth += a6;
            }
            println!(
                "average_height: {:.2}, depth: {:.2}",
                average_height / len as f64,
                depth as f64 / len as f64
            )
        }
    }
}

criterion_group!(
    benches,
    blocksize,
    skewness,
    parallelism_of_first_committer_wins_rule
);

criterion_main!(benches);
