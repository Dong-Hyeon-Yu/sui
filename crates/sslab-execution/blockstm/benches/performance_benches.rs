use std::sync::Arc;

use criterion::Throughput;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use ethers_providers::{MockProvider, Provider};

use sslab_execution::executor::Executable;
use sslab_execution::types::{EthereumTransaction, ExecutableEthereumBatch};
use sslab_execution::utils::test_utils::SmallBankTransactionHandler;
use sslab_execution_blockstm::utils::smallbank_contract_benchmark::concurrent_evm_storage;
use sslab_execution_blockstm::BlockSTM;

const DEFAULT_BATCH_SIZE: usize = 200;
// const DEFAULT_BLOCK_CONCURRENCY: u64 = 12;
const DEFAULT_SKEWNESS: f32 = 0.0;
const DEFAULT_CHAIN_ID: u64 = 9;

fn _get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn _get_blockstm_executor() -> BlockSTM {
    BlockSTM::new(Arc::new(concurrent_evm_storage()))
}

fn _create_random_smallbank_workload(
    skewness: f32,
    batch_size: usize,
    block_concurrency: usize,
) -> Vec<ExecutableEthereumBatch<EthereumTransaction>> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, 10_000)
}

fn batch_size(c: &mut Criterion) {
    let param = 1..81;
    let mut group = c.benchmark_group("BlockSTM Benchmark according to batch size");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("blockstm with #batch", i),
            &i,
            |b, i| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter_batched(
                        || {
                            let consensus_output = _create_random_smallbank_workload(
                                DEFAULT_SKEWNESS,
                                DEFAULT_BATCH_SIZE * i,
                                1,
                            );
                            let blockstm = _get_blockstm_executor();
                            (blockstm, consensus_output)
                        },
                        |(blockstm, consensus_output)| async move {
                            let _ = blockstm.execute(consensus_output).await;
                        },
                        BatchSize::SmallInput,
                    );
            },
        );
    }
}

criterion_group!(benches, batch_size);
criterion_main!(benches);
