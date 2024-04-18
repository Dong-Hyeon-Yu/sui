use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use ethers_providers::{MockProvider, Provider};
use parking_lot::RwLock;

use sslab_execution::types::ExecutableEthereumBatch;
use sslab_execution::utils::test_utils::SmallBankTransactionHandler;
use sslab_execution_blockstm::utils::smallbank_contract_benchmark::concurrent_evm_storage;
use sslab_execution_blockstm::BlockSTM;
use tokio::time::Instant;

const DEFAULT_BATCH_SIZE: usize = 200;
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
    account_num: u64,
) -> Vec<ExecutableEthereumBatch> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, account_num)
}

fn latency(c: &mut Criterion) {
    let account_nums = [400];
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 1..2;
    let mut group = c.benchmark_group("BlockSTM");

    for account_num in account_nums {
        for i in param.clone() {
            for skewness in s {
                let latency_metrics = std::sync::Arc::new(RwLock::new(Vec::new()));

                group.bench_with_input(
                    criterion::BenchmarkId::new(
                        "blockstm",
                        format!("(#account: {account_num}, #batch: {i}, zipfian: {skewness})"),
                    ),
                    &(i, latency_metrics.clone()),
                    |b, (i, latency_metrics)| {
                        b.to_async(tokio::runtime::Runtime::new().unwrap())
                            .iter_batched(
                                || {
                                    let consensus_output = _create_random_smallbank_workload(
                                        skewness,
                                        DEFAULT_BATCH_SIZE * i,
                                        1,
                                        account_num,
                                    );
                                    let blockstm = _get_blockstm_executor();
                                    (blockstm, consensus_output)
                                },
                                |(blockstm, consensus_output)| async move {
                                    let latency = Instant::now();
                                    let c = blockstm
                                        .execute_and_return_commit_latency(consensus_output)
                                        .await;
                                    latency_metrics
                                        .write()
                                        .push((latency.elapsed().as_millis(), c));
                                },
                                BatchSize::SmallInput,
                            );
                    },
                );

                let (mut total, mut commit) = (0 as f64, 0 as f64);
                let len = latency_metrics.read().len();
                for (t, c) in latency_metrics.read().iter() {
                    total += *t as f64;

                    commit += *c as f64;
                }
                total /= len as f64;
                commit /= len as f64;
                let execution = total - commit;

                println!(
                    "total: {:.4}, execution: {:.4}, commit: {:.4}",
                    total, execution, commit,
                );
                println!("Ktps: {:.4}", (DEFAULT_BATCH_SIZE * i) as f64 / total);
            }
        }
    }
}

criterion_group!(blockstm, latency);
criterion_main!(blockstm);
