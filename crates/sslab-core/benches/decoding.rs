#![allow(dead_code)]
use criterion::Throughput;
use criterion::{criterion_group, criterion_main, Criterion, BatchSize};

use ethers_core::types::transaction::eip2718::TypedTransaction;
use ethers_providers::{MockProvider, Provider};
use narwhal_types::BatchDigest;
use rayon::prelude::*;
use sslab_core::consensus_handler::decode_transaction;
use sslab_execution::types::{ExecutableEthereumBatch, EthereumTransaction};
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

fn _create_random_smallbank_workload_v2(skewness: f32, batch_size: usize, block_concurrency: usize) -> Vec<ExecutableEthereumBatch> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, 10_000)
}

fn _rlp_decode(serialized_transaction: &Vec<u8>) -> TypedTransaction {
    let rlp = ethers_core::utils::rlp::Rlp::new(serialized_transaction);
    TypedTransaction::decode_signed(&rlp).unwrap().0
}


fn rlp_decoding(c: &mut Criterion) {
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
                                    let _ = _rlp_decode(&bytes);
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

fn _json_encode(transaction: &EthereumTransaction) -> Vec<u8> {
    serde_json::to_vec(&transaction.0).unwrap()
}

fn _json_decode(serialized_transaction: &Vec<u8>) -> TypedTransaction {
    serde_json::from_slice::<TypedTransaction>(serialized_transaction).unwrap()
}

fn json_decoding(c: &mut Criterion) {
    let param = 1..11;
    let mut group = c.benchmark_group("decoding ethereum transactions according to # of batches");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("# of batches", i),
            &i,
            |b, i| {
                b.iter_batched(
                    || {
                        let batches = _create_random_smallbank_workload_v2(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
                        batches
                            .into_par_iter()
                            .map(|batch| 
                                batch.data().iter().map(|tx| _json_encode(tx)).collect::<Vec<_>>()
                            )
                            .collect::<Vec<_>>()
                    },
                    |batches| {
                        for batch in batches {
                            batch
                                .par_iter()
                                .map(|serialized_transaction| {
                                    _json_decode(serialized_transaction)
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

fn decoding_in_consensus_handler(c: &mut Criterion) {
    let param = 1..11;
    let mut group = c.benchmark_group("decoding ethereum transactions according to # of batches");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("# of batches", i),
            &i,
            |b, i| {
                b.iter_batched(
                    || {
                        let batches = _create_random_smallbank_workload_v2(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
                        batches
                            .into_par_iter()
                            .map(|batch| 
                                batch.data().iter().map(|tx| _json_encode(tx)).collect::<Vec<_>>()
                            )
                            .collect::<Vec<_>>()
                    },
                    |batches| {
                        for batch in batches {
                            batch
                                .par_iter()
                                .map(|serialized_transaction| {
                                    decode_transaction(serialized_transaction, BatchDigest::default())
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

// TODO: bincode::deserialize is not working
// fn _bincode_encode(transaction: &EthereumTransaction) -> Vec<u8> {
//     bincode::serialize(&transaction.0).unwrap()
// }

// fn _bincode_decode(serialized_transaction: &Vec<u8>) -> TypedTransaction {
//     bincode::deserialize_from(serialized_transaction.clone().as_slice()).unwrap()
// }

// fn bincode_decoding(c: &mut Criterion) {
//     let param = 1..11;
//     let mut group = c.benchmark_group("decoding ethereum transactions according to # of batches");
//     for i in param {
//         group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
//         group.bench_with_input(
//             criterion::BenchmarkId::new("# of batches", i),
//             &i,
//             |b, i| {
//                 b.iter_batched(
//                     || {
//                         let batches = _create_random_smallbank_workload_v2(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
//                         batches
//                             .into_par_iter()
//                             .map(|batch| 
//                                 batch.data().iter().map(|tx| _bincode_encode(tx)).collect::<Vec<_>>()
//                             )
//                             .collect::<Vec<_>>()
//                     },
//                     |batches| {
//                         for batch in batches {
//                             batch
//                                 .par_iter()
//                                 .map(|serialized_transaction| {
//                                     _bincode_decode(serialized_transaction)
//                                 })
//                                 .collect::<Vec<_>>();
//                         }
//                     },
//                     BatchSize::SmallInput
//                 );
//             }
//         );
//     }
// }


criterion_group!(benches, decoding_in_consensus_handler);
criterion_main!(benches);

