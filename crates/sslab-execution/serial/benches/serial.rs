use std::sync::Arc;

use criterion::Throughput;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use ethers_providers::{MockProvider, Provider};
use sslab_execution::types::ExecutableEthereumBatch;
use sslab_execution::utils::smallbank_contract_benchmark::concurrent_evm_storage;
use sslab_execution::utils::test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID};
use sslab_execution_serial::SerialExecutor;

const DEFAULT_BATCH_SIZE: usize = 200;

fn _get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn _get_serial_executor() -> SerialExecutor {
    let memory_storage = Arc::new(concurrent_evm_storage());
    SerialExecutor::new(memory_storage)
}

fn _create_random_smallbank_workload(
    skewness: f32,
    batch_size: usize,
    block_concurrency: usize,
) -> Vec<ExecutableEthereumBatch> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, 100_000)
}

fn serial(c: &mut Criterion) {
    let s = [0.0];
    let param = 1..81;
    let mut group = c.benchmark_group("Serial");
    for skewness in s {
        for i in param.clone() {
            group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));
            group.bench_with_input(criterion::BenchmarkId::new("blocksize", i), &i, |b, i| {
                b.iter_batched(
                    || {
                        let consensus_output =
                            _create_random_smallbank_workload(skewness, DEFAULT_BATCH_SIZE * i, 1);
                        let serial = _get_serial_executor();
                        (serial, consensus_output)
                    },
                    |(serial, consensus_output)| {
                        consensus_output.into_iter().for_each(|batch| {
                            serial._execute(batch);
                        });
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }
}

fn skewness(c: &mut Criterion) {
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 80..81;
    let mut group = c.benchmark_group("Serial");
    for skewness in s {
        for i in param.clone() {
            group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));
            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "skewness",
                    format!("zipfian: {}, block_size: {}", skewness, i),
                ),
                &i,
                |b, i| {
                    b.iter_batched(
                        || {
                            let consensus_output =
                                _create_random_smallbank_workload(0.0, DEFAULT_BATCH_SIZE * i, 1);
                            let serial = _get_serial_executor();
                            (serial, consensus_output)
                        },
                        |(serial, consensus_output)| {
                            consensus_output.into_iter().for_each(|batch| {
                                serial._execute(batch);
                            });
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
}

criterion_group!(benches, serial, skewness);
criterion_main!(benches);
