use std::sync::Arc;

use criterion::Throughput;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use ethers_providers::{MockProvider, Provider};

use sslab_execution::executor::Executable;
use sslab_execution::types::ExecutableEthereumBatch;
use sslab_execution::utils::test_utils::SmallBankTransactionHandler;
use sslab_execution_blockstm::utils::smallbank_contract_benchmark::concurrent_evm_storage;
use sslab_execution_blockstm::BlockSTM;

const DEFAULT_BATCH_SIZE: usize = 200;
const DEFAULT_CHAIN_ID: u64 = 9;
const DEFAULT_ACCOUNT_NUM: u64 = 100000;

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
    account_num: u64,
) -> Vec<ExecutableEthereumBatch> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, account_num)
}

fn batch(c: &mut Criterion) {
    let s = [0.0];
    let param = 1..81;
    let mut group = c.benchmark_group("BlockSTM");

    for skewness in s {
        for i in param.clone() {
            group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));
            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "blocksize",
                    format!("zipfian: {skewness}, #batch: {i}"),
                ),
                &i,
                |b, i| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || {
                                let consensus_output = _create_random_smallbank_workload(
                                    skewness,
                                    DEFAULT_BATCH_SIZE * i,
                                    1,
                                    DEFAULT_ACCOUNT_NUM,
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
}

fn skewness(c: &mut Criterion) {
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 80..81;
    let mut group = c.benchmark_group("BlockSTM");

    for skewness in s {
        for i in param.clone() {
            group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));
            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "skewness",
                    format!("zipfian: {skewness}, #batch: {i}"),
                ),
                &i,
                |b, i| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || {
                                let consensus_output = _create_random_smallbank_workload(
                                    skewness,
                                    DEFAULT_BATCH_SIZE * i,
                                    1,
                                    DEFAULT_ACCOUNT_NUM,
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
}

criterion_group!(blockstm, batch, skewness);
criterion_main!(blockstm);
