use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use ethers_providers::{MockProvider, Provider};
use parking_lot::RwLock;
use reth::primitives::TransactionSignedEcRecovered;
use sslab_execution::{
    types::ExecutableEthereumBatch,
    utils::smallbank_contract_benchmark::concurrent_memory_database,
    utils::test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID},
};

use sslab_execution_nezha::{nezha_core::LatencyBenchmark as _, ConcurrencyLevelManager};
const DEFAULT_BATCH_SIZE: usize = 200;

fn _get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn _get_nezha_executor(clevel: usize) -> ConcurrencyLevelManager {
    ConcurrencyLevelManager::new(concurrent_memory_database(), clevel)
}

fn _create_random_smallbank_workload(
    skewness: f32,
    batch_size: usize,
    block_concurrency: usize,
    account_num: u64,
) -> Vec<ExecutableEthereumBatch<TransactionSignedEcRecovered>> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, account_num)
}

fn optme_latency_inspection(c: &mut Criterion) {
    let account_nums = [100_000];
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 80..81;
    let mut group = c.benchmark_group("Latency");

    for account_num in account_nums {
        for i in param.clone() {
            for zipfian in s {
                let latency_metrics = std::sync::Arc::new(RwLock::new(Vec::new()));

                group.bench_with_input(
                    criterion::BenchmarkId::new(
                        "optme",
                        format!(
                            "(#account: {account_num}, block concurrency: {i}, zipfian: {zipfian})"
                        ),
                    ),
                    &(i, latency_metrics.clone()),
                    |b, (i, latency_metrics)| {
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
                                    latency_metrics.write().push(
                                        nezha._execute_and_return_latency(consensus_output).await,
                                    );
                                },
                                BatchSize::SmallInput,
                            );
                    },
                );
                let len = latency_metrics.read().len() as f64;
                if len == 0.0 {
                    continue;
                }

                let (
                    mut total,
                    mut simulation,
                    mut scheduling,
                    mut validation,
                    mut validation_execute,
                    mut commit,
                ) = (0 as f64, 0 as f64, 0 as f64, 0 as f64, 0 as f64, 0 as f64);

                for (a1, a2, a3, a4, a5) in latency_metrics.read().iter() {
                    total += *a1 as f64;
                    simulation += *a2 as f64;
                    scheduling += *a3 as f64;
                    validation += a4.0 as f64;
                    validation_execute += a4.1 as f64;
                    commit += *a5 as f64;
                }
                total /= len;
                simulation /= len;
                scheduling /= len;
                validation /= len;
                validation_execute /= len;
                commit /= len;
                let other =
                    total - (simulation + scheduling + validation + validation_execute + commit);

                println!(
                    "Total: {:.4}, Simulation: {:.4}, Scheduling: {:.4}, Validation: (execute: {:.4}, validate: {:.4}), Commit: {:.4}, Other: {:.4}",
                    total /1000.0, simulation /1000.0, scheduling/1000.0, validation_execute/1000.0, validation/1000.0, commit/1000.0, other/1000.0
                );
                println!(
                    "Ktps: {:.4}",
                    (DEFAULT_BATCH_SIZE * i) as f64 / (total / 1000.0)
                )
            }
        }
    }
}

criterion_group!(benches, optme_latency_inspection);
criterion_main!(benches);
