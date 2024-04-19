use criterion::Throughput;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use ethers_providers::{MockProvider, Provider};
use reth::primitives::TransactionSignedEcRecovered;
use sslab_execution::executor::Executable;
use sslab_execution::types::ExecutableEthereumBatch;
use sslab_execution::utils::{
    smallbank_contract_benchmark::concurrent_memory_database,
    test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID},
};
use sslab_execution_serial::SerialExecutor;

const DEFAULT_BATCH_SIZE: usize = 200;

fn _get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn _get_serial_executor() -> SerialExecutor {
    let memory_storage = concurrent_memory_database();
    SerialExecutor::new(memory_storage)
}

fn _create_random_smallbank_workload(
    skewness: f32,
    batch_size: usize,
    block_concurrency: usize,
) -> Vec<ExecutableEthereumBatch<TransactionSignedEcRecovered>> {
    let handler = _get_smallbank_handler();

    handler.create_batches_v2(batch_size, block_concurrency, skewness, 100_000)
}

fn serial(c: &mut Criterion) {
    let param = 1..81;
    let mut group = c.benchmark_group("Serial");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));
        group.bench_with_input(criterion::BenchmarkId::new("blocksize", i), &i, |b, i| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter_batched(
                    || {
                        let consensus_output =
                            _create_random_smallbank_workload(0.0, DEFAULT_BATCH_SIZE, *i);
                        let serial = _get_serial_executor();
                        (serial, consensus_output)
                    },
                    |(serial, consensus_output)| async move {
                        let _ = serial.execute(consensus_output).await;
                    },
                    BatchSize::SmallInput,
                );
        });
    }
}

criterion_group!(benches, serial);
criterion_main!(benches);
