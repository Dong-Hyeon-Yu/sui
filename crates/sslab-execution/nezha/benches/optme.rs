#![allow(dead_code)]
use criterion::Throughput;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use reth::primitives::{BlockWithSenders, ChainSpec};
use reth::providers::ChainSpecProvider;
use sslab_execution::ProviderFactoryMDBX;
use sslab_execution::{
    get_provider_factory,
    traits::Executable,
    utils::smallbank_contract_benchmark::{
        cache_state_with_smallbank_contract, get_smallbank_handler,
    },
    utils::test_utils::{convert_into_block, default_chain_spec},
};
use std::sync::Arc;

use sslab_execution_nezha::OptME;

const DEFAULT_BATCH_SIZE: usize = 200;
const DEFAULT_BLOCK_CONCURRENCY: usize = 12;
const DEFAULT_SKEWNESS: f32 = 0.0;

fn _get_executor(provider_factory: ProviderFactoryMDBX) -> OptME {
    let chain_spec = provider_factory.chain_spec();
    OptME::new_with_db(
        provider_factory,
        Some(cache_state_with_smallbank_contract()),
        chain_spec,
    )
}

fn _create_random_smallbank_workload(
    skewness: f32,
    batch_size: usize,
    block_concurrency: usize,
) -> BlockWithSenders {
    let handler = get_smallbank_handler();

    convert_into_block(handler.create_batches(batch_size, block_concurrency, skewness, 100_000))
}

fn optme(c: &mut Criterion) {
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 1..81;
    let mut group = c.benchmark_group("OptME");

    let chain_spec = Arc::new(default_chain_spec());
    let provider_factory = get_provider_factory(chain_spec.clone());

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
                                let optme = _get_executor(provider_factory.clone());
                                (optme, consensus_output)
                            },
                            |(mut optme, consensus_output)| async move {
                                optme._execute_inner(consensus_output).await
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

    let chain_spec = Arc::new(default_chain_spec());
    let provider_factory = get_provider_factory(chain_spec.clone());

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
                                let optme = _get_executor(provider_factory.clone());
                                (optme, consensus_output)
                            },
                            |(mut optme, consensus_output)| async move {
                                optme._execute_inner(consensus_output).await
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
