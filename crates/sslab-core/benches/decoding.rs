use criterion::Throughput;
use criterion::{criterion_group, criterion_main, Criterion, BatchSize};

use ethers_providers::{MockProvider, Provider};
use narwhal_types::BatchDigest;
use rayon::prelude::*;
use sslab_core::consensus_handler::decode_transaction;
use sslab_execution::utils::test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID};

const DEFAULT_BATCH_SIZE: usize = 250;
const DEFAULT_SKEWNESS: f32 = 0.0;


fn _get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn _create_random_smallbank_workload(skewness: f32, batch_size: usize, block_concurrency: usize) -> Vec<Vec<bytes::Bytes>> {
    let handler = _get_smallbank_handler();

    handler.create_raw_batches(batch_size, block_concurrency, skewness, 10_000)
}

fn decoding(c: &mut Criterion) {
    let param = 1..5;
    let mut group = c.benchmark_group("decoding ethereum transactions according to # of batches");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("# of batches", i),
            &i,
            |b, i| {
                b.iter_batched(
                    || {
                        _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i)
                    },
                    |batches| {
                        for batch in batches {
                            batch
                                .par_iter()
                                .map(|serialized_transaction| {
                                    let bytes = serialized_transaction.to_vec();
                                    decode_transaction(&bytes, BatchDigest::default())
                                })
                                .collect::<Vec<_>>();
                        }
                    },
                    BatchSize::SmallInput
                );
            }
        );
    }
}

criterion_group!(benches, decoding);
criterion_main!(benches);