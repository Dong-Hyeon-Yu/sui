use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use ethers_providers::{MockProvider, Provider};
use parking_lot::RwLock;
use sslab_execution::{
    types::ExecutableEthereumBatch,
    utils::smallbank_contract_benchmark::concurrent_evm_storage,
    utils::test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID},
};

use sslab_execution_nezha::{
    nezha_core::Benchmark, ConcurrencyLevelManager, SimulatedTransaction, SimulationResult,
};

const DEFAULT_BATCH_SIZE: usize = 200;
const DEFAULT_ACCOUNT_NUM: u64 = 100_000;

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
    account_num: u64,
) -> Vec<ExecutableEthereumBatch> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, account_num)
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

fn parallelism_of_optme(c: &mut Criterion) {
    let account_num = 400;
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 1..2;
    let mut group = c.benchmark_group("OptME");

    for i in param {
        for zipfian in s {
            let parallelism_metrics = std::sync::Arc::new(RwLock::new(Vec::new()));

            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "optme",
                    format!("(zipfian: {}, block_concurrency: {})", zipfian, i),
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
                                    account_num,
                                );
                                let nezha = _get_nezha_executor(*i);
                                (nezha, consensus_output)
                            },
                            |(nezha, consensus_output)| async move {
                                metrics.write().push(
                                    nezha._analysis_parallelism_of_optme(consensus_output).await,
                                );
                            },
                            BatchSize::SmallInput,
                        );
                },
            );

            let len = parallelism_metrics.read().len();

            if len == 0 {
                continue;
            }

            let (
                // mut total_tx,
                mut average_height,
                // mut std_height,
                // mut skewness_height,
                // mut max_height,
                mut depth,
            ) = (0 as f64, 0 as u32);

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

fn parallelism_of_first_committer_wins_rule(c: &mut Criterion) {
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 80..81;
    let mut group = c.benchmark_group("Nezha(FUW)");

    for i in param {
        for zipfian in s {
            let parallelism_metrics = std::sync::Arc::new(RwLock::new(Vec::new()));

            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "nezha-with-FUW",
                    format!("(zipfian: {}, block_concurrency: {})", zipfian, i),
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
                                    DEFAULT_ACCOUNT_NUM,
                                );
                                let nezha = _get_nezha_executor(*i);
                                (nezha, consensus_output)
                            },
                            |(nezha, consensus_output)| async move {
                                metrics.write().push(
                                    nezha
                                        ._analysis_parallelism_of_nezha_with_fuw(consensus_output)
                                        .await,
                                );
                            },
                            BatchSize::SmallInput,
                        );
                },
            );

            let len = parallelism_metrics.read().len();

            if len == 0 {
                continue;
            }

            let (
                // mut total_tx,
                mut average_height,
                // mut std_height,
                // mut skewness_height,
                // mut max_height,
                mut depth,
            ) = (0 as f64, 0 as u32);

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
    parallelism_of_optme,
    parallelism_of_first_committer_wins_rule
);
criterion_main!(benches);
