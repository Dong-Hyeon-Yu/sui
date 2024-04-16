use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use ethers_providers::{MockProvider, Provider};
use parking_lot::RwLock;
use sslab_execution::{
    types::ExecutableEthereumBatch,
    utils::smallbank_contract_benchmark::concurrent_evm_storage,
    utils::test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID},
};

use sslab_execution_nezha::{
    address_based_conflict_graph::Benchmark as _, AddressBasedConflictGraph,
    ConcurrencyLevelManager, SimulatedTransaction, SimulationResult,
};
const DEFAULT_BATCH_SIZE: usize = 200;

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
        let SimulationResult { rw_sets, .. } = nezha.simulate(consensus_output).await;
        tx.send(rw_sets).unwrap();
    });
    rx.recv().unwrap()
}

fn baseline(c: &mut Criterion) {
    let s = [0.5, 1.0];
    let param = 80..81;
    let mut group = c.benchmark_group("Nezha(FUW)");

    for zipfian in s {
        for i in param.clone() {
            let duration_metrics = std::sync::Arc::new(RwLock::new(Vec::new()));

            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "baseline",
                    format!("(zipfian: {}, blocksize: {})", zipfian, i),
                ),
                &(i, duration_metrics.clone()),
                |b, (i, metrics)| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || {
                                let consensus_output = _create_random_smallbank_workload(
                                    zipfian,
                                    DEFAULT_BATCH_SIZE,
                                    *i,
                                );
                                let nezha = std::sync::Arc::new(_get_nezha_executor(*i));
                                let rw_sets = _get_rw_sets(nezha.clone(), consensus_output.clone());
                                rw_sets
                            },
                            |rw_sets| async move {
                                let now = tokio::time::Instant::now();
                                let mut acg =
                                    AddressBasedConflictGraph::construct_without_early_detection(
                                        rw_sets,
                                    );
                                let construction = now.elapsed().as_micros() as f64 / 1000f64;

                                let now = tokio::time::Instant::now();
                                acg.hierarchcial_sort();
                                let sorting = now.elapsed().as_micros() as f64 / 1000f64;

                                let now = tokio::time::Instant::now();
                                acg.reorder();
                                let reordering = now.elapsed().as_micros() as f64 / 1000f64;

                                let _ = acg.par_extract_schedule().await;

                                metrics.write().push((construction, sorting, reordering))
                                // metrics.write().unwrap().push(result.parallism_metric())
                            },
                            BatchSize::SmallInput,
                        );
                },
            );

            let (mut construction, mut sorting, mut reordering) = (0 as f64, 0 as f64, 0 as f64);
            let len = duration_metrics.read().len() as f64;

            for (a1, a2, a3) in duration_metrics.read().iter() {
                construction += a1;
                sorting += a2;
                reordering += a3;
            }

            println!("ACG construct: {:.4}", construction / len);
            println!("Hierachical sort: {:.4}", sorting / len);
            println!("Reorder: {:.4}", reordering / len);
        }
    }
}

fn early_detection(c: &mut Criterion) {
    let s = [0.5, 1.0];
    let param = 80..81;
    let mut group = c.benchmark_group("Nezha(FUW)");

    for zipfian in s {
        for i in param.clone() {
            let duration_metrics = std::sync::Arc::new(RwLock::new(Vec::new()));

            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "early-detection",
                    format!("(zipfian: {}, blocksize: {})", zipfian, i),
                ),
                &(i, duration_metrics.clone()),
                |b, (i, metrics)| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || {
                                let consensus_output = _create_random_smallbank_workload(
                                    zipfian,
                                    DEFAULT_BATCH_SIZE,
                                    *i,
                                );
                                let nezha = std::sync::Arc::new(_get_nezha_executor(*i));
                                let rw_sets = _get_rw_sets(nezha.clone(), consensus_output.clone());
                                rw_sets
                            },
                            |rw_sets| async move {
                                let now = tokio::time::Instant::now();
                                let mut acg = AddressBasedConflictGraph::construct(rw_sets);
                                let construction = now.elapsed().as_micros() as f64 / 1000f64;

                                let now = tokio::time::Instant::now();
                                acg.hierarchcial_sort();
                                let sorting = now.elapsed().as_micros() as f64 / 1000f64;

                                let now = tokio::time::Instant::now();
                                acg.reorder();
                                let reordering = now.elapsed().as_micros() as f64 / 1000f64;

                                let _ = acg.par_extract_schedule().await;

                                metrics.write().push((construction, sorting, reordering))
                                // metrics.write().unwrap().push(result.parallism_metric())
                            },
                            BatchSize::SmallInput,
                        );
                },
            );

            let (mut construction, mut sorting, mut reordering) = (0 as f64, 0 as f64, 0 as f64);
            let len = duration_metrics.read().len() as f64;

            for (a1, a2, a3) in duration_metrics.read().iter() {
                construction += a1;
                sorting += a2;
                reordering += a3;
            }

            println!("ACG construct: {:.4}", construction / len);
            println!("Hierachical sort: {:.4}", sorting / len);
            println!("Reorder: {:.4}", reordering / len);
        }
    }
}

fn parallel_early_detection(c: &mut Criterion) {
    let s = [0.5, 1.0];
    let param = 80..81;
    let mut group = c.benchmark_group("Nezha(FUW)");

    for zipfian in s {
        for i in param.clone() {
            let duration_metrics = std::sync::Arc::new(RwLock::new(Vec::new()));

            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "parallel-early-detection",
                    format!("(zipfian: {}, blocksize: {})", zipfian, i),
                ),
                &(i, duration_metrics.clone()),
                |b, (i, metrics)| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || {
                                let consensus_output = _create_random_smallbank_workload(
                                    zipfian,
                                    DEFAULT_BATCH_SIZE,
                                    *i,
                                );
                                let nezha = std::sync::Arc::new(_get_nezha_executor(*i));
                                let rw_sets = _get_rw_sets(nezha.clone(), consensus_output.clone());
                                rw_sets
                            },
                            |rw_sets| async move {
                                let now = tokio::time::Instant::now();
                                let mut acg =
                                    AddressBasedConflictGraph::par_construct(rw_sets).await;
                                let construction = now.elapsed().as_micros() as f64 / 1000f64;

                                let now = tokio::time::Instant::now();
                                acg.hierarchcial_sort();
                                let sorting = now.elapsed().as_micros() as f64 / 1000f64;

                                let now = tokio::time::Instant::now();
                                acg.reorder();
                                let reordering = now.elapsed().as_micros() as f64 / 1000f64;

                                let _ = acg.par_extract_schedule().await;

                                metrics.write().push((construction, sorting, reordering))
                                // metrics.write().unwrap().push(result.parallism_metric())
                            },
                            BatchSize::SmallInput,
                        );
                },
            );

            let (mut construction, mut sorting, mut reordering) = (0 as f64, 0 as f64, 0 as f64);
            let len = duration_metrics.read().len() as f64;

            for (a1, a2, a3) in duration_metrics.read().iter() {
                construction += a1;
                sorting += a2;
                reordering += a3;
            }

            println!("ACG construct: {:.4}", construction / len);
            println!("Hierachical sort: {:.4}", sorting / len);
            println!("Reorder: {:.4}", reordering / len);
        }
    }
}

criterion_group!(benches, baseline, early_detection, parallel_early_detection);
criterion_main!(benches);
