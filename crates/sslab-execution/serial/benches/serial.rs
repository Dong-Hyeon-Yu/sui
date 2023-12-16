use std::sync::Arc;

use criterion::Throughput;
use criterion::{criterion_group, criterion_main, Criterion, BatchSize};
use ethers_providers::{MockProvider, Provider};
use sslab_execution::types::ExecutableEthereumBatch;
use sslab_execution::utils::smallbank_contract_benchmark::default_memory_storage;
use sslab_execution::utils::test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID};
use sslab_execution_serial::SerialExecutor;

const DEFAULT_BATCH_SIZE: usize = 200;

fn _get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn _get_serial_executor() -> SerialExecutor {
    let memory_storage = Arc::new(default_memory_storage());
    SerialExecutor::new(memory_storage)
}

fn _create_random_smallbank_workload(skewness: f32, batch_size: usize, block_concurrency: usize) -> Vec<ExecutableEthereumBatch> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, 10_000)
}


fn serial(c: &mut Criterion) {
    let param = 1..41;
    let mut group = c.benchmark_group("Serial execution Benchmark according to batchsize");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("serial execution", i),
            &i,
            |b, i| {
                b.iter_batched(
                    || {
                        let consensus_output = _create_random_smallbank_workload(0.0, DEFAULT_BATCH_SIZE*i, 1);
                        let serial = _get_serial_executor();
                        (serial, consensus_output)
                    },
                    |(serial, consensus_output)| {
                        consensus_output
                            .into_iter()
                            .for_each(|batch| {
                                serial._execute(batch);
                            });
                    },
                    BatchSize::SmallInput
                );
            }
        );
    }
}

criterion_group!(benches, serial);
// criterion_group!(benches, simulation, nezha, commit, block_concurrency);
criterion_main!(benches);