#![allow(dead_code)]
use criterion::Throughput;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use ethers_providers::{MockProvider, Provider};

use reth::primitives::TransactionSignedEcRecovered;
use sslab_execution::{
    types::ExecutableEthereumBatch,
    utils::smallbank_contract_benchmark::concurrent_memory_database,
    utils::test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID},
};

use sslab_execution_nezha::ConcurrencyLevelManager;

const DEFAULT_BATCH_SIZE: usize = 200;
const DEFAULT_BLOCK_CONCURRENCY: usize = 12;
const DEFAULT_SKEWNESS: f32 = 0.0;

fn _get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn _get_nezha_executor(clevel: usize) -> ConcurrencyLevelManager<InMemoryConcurrentDB> {
    ConcurrencyLevelManager::new(concurrent_memory_database(), clevel)
}

fn _create_random_smallbank_workload(
    skewness: f32,
    batch_size: usize,
    block_concurrency: usize,
) -> Vec<ExecutableEthereumBatch<TransactionSignedEcRecovered>> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, 100_000)
}

fn optme(c: &mut Criterion) {
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 1..81;
    let mut group = c.benchmark_group("OptME");

    for zipfian in s {
        for i in param.clone() {
            group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));
            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "blocksize",
                    format!("(zipfian: {}, block_concurrency: {})", zipfian, i),
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
                                nezha._execute(consensus_output).await
                            },
                            BatchSize::SmallInput,
                        );
                },
            );
        }
    }
}

fn optme_skewness(c: &mut Criterion) {
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 80..81;
    let mut group = c.benchmark_group("OptME");

    for zipfian in s {
        for i in param.clone() {
            group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));
            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "skewness",
                    format!("(zipfian: {}, block_concurrency: {})", zipfian, i),
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
                                nezha._execute(consensus_output).await
                            },
                            BatchSize::SmallInput,
                        );
                },
            );
        }
    }
}

criterion_group!(benches, optme, optme_skewness);
criterion_main!(benches);
