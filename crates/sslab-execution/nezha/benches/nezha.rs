#![allow(dead_code)]
use criterion::Throughput;
use criterion::{criterion_group, criterion_main, Criterion, BatchSize};
use ethers_providers::{Provider, MockProvider};
use narwhal_types::BatchDigest;

use rayon::iter::IntoParallelIterator;
use rayon::prelude::*;

use sslab_execution::{
    types::ExecutableEthereumBatch,
    utils::smallbank_contract_benchmark::concurrent_evm_storage,
    utils::test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID}
};

use sslab_execution_nezha::{
    AddressBasedConflictGraph,
    ConcurrencyLevelManager,
    SimulationResult,
};

const DEFAULT_BATCH_SIZE: u64 = 200;
const DEFAULT_BLOCK_CONCURRENCY: u64 = 12;
const DEFAULT_SKEWNESS: f32 = 0.0;

fn _get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn _get_nezha_executor() -> ConcurrencyLevelManager {
    ConcurrencyLevelManager::new(concurrent_evm_storage(), 20)
}

fn _create_random_smallbank_workload(skewness: f32, batch_size: u64, block_concurrency: u64) -> Vec<ExecutableEthereumBatch> {
    let handler = _get_smallbank_handler();

    (0..block_concurrency)
        .into_iter()
        .map(|_| {
            let tmp = (0..batch_size).into_par_iter().map(|_|
                handler.random_operation(skewness, 10_000)
            ).collect();
            ExecutableEthereumBatch::new(tmp, BatchDigest::default())
        })
        .collect()
}

fn block_concurrency(c: &mut Criterion) {
    let param = 21..40;
    let mut group = c.benchmark_group("Nezha Benchmark according to block concurrency");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("nezha", i),
            &i,
            |b, i| {
                b.iter_batched(
                    || {
                        let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
                        let nezha = _get_nezha_executor();
                        (nezha, consensus_output)
                    },
                    |(nezha, consensus_output)| {
                        let SimulationResult { rw_sets, .. } = nezha._simulate(consensus_output);
                        let scheduled_info = AddressBasedConflictGraph::construct(rw_sets)
                            .hierarchcial_sort()
                            .reorder()
                            .extract_schedule();
                        nezha._concurrent_commit(scheduled_info, 1)
                    },
                    BatchSize::SmallInput
                );
            }
        );
    }
}

fn block_concurrency_simulation(c: &mut Criterion) {
    let param = 1..41;
    let mut group = c.benchmark_group("simulation according to block concurrency");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("nezha", i),
            &i,
            |b, i| {
                b.iter_batched(
                    || {
                        let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
                        let nezha = _get_nezha_executor();
                        (nezha, consensus_output)
                    },
                    |(nezha, consensus_output)| {
                        let _ = nezha._simulate(consensus_output);
                        // let scheduled_info = AddressBasedConflictGraph::construct(rw_sets)
                        //     .hierarchcial_sort()
                        //     .reorder()
                        //     .extract_schedule();
                        // nezha._concurrent_commit(scheduled_info, 1)
                    },
                    BatchSize::SmallInput
                );
            }
        );
    }
}

fn block_concurrency_scheduling(c: &mut Criterion) {
    let param = 1..41;
    let mut group = c.benchmark_group("scheduling according to block concurrency");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("nezha", i),
            &i,
            |b, i| {
                b.iter_batched(
                    || {
                        let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
                        let nezha = _get_nezha_executor();
                        let SimulationResult { rw_sets, .. } = nezha._simulate(consensus_output);
                        rw_sets
                    },
                    |rw_sets| {
                        let _ = AddressBasedConflictGraph::construct(rw_sets)
                            .hierarchcial_sort()
                            .reorder()
                            .extract_schedule();
                    },
                    BatchSize::SmallInput
                );
            }
        );
    }
}

fn block_concurrency_commit(c: &mut Criterion) {
    let param = 1..41;
    let mut group = c.benchmark_group("concurrent commit according to block concurrency");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("nezha", i),
            &i,
            |b, i| {
                b.iter_batched(
                    || {
                        let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
                        let nezha = _get_nezha_executor();
                        let SimulationResult { rw_sets, .. } = nezha._simulate(consensus_output);
                        let scheduled_info = AddressBasedConflictGraph::construct(rw_sets)
                            .hierarchcial_sort()
                            .reorder()
                            .extract_schedule();
                        (nezha, scheduled_info)
                    },
                    |(nezha, scheduled_info)| {
                        nezha._concurrent_commit(scheduled_info, 1);
                    },
                    BatchSize::SmallInput
                );
            }
        );
    }
}


fn simulation(c: &mut Criterion) {

    c.bench_function("simulation ethereum trasactions", |b| {

        b.iter_batched(
            || {
                let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, DEFAULT_BLOCK_CONCURRENCY);
                let nezha = _get_nezha_executor();
                (nezha, consensus_output)
            },
            |(nezha, consensus_output)| {
                let _rwset = nezha._simulate(consensus_output);
            },
            BatchSize::SmallInput
        );
    });
}

fn nezha(c: &mut Criterion) {
    c.bench_function("nezha algorithm", |b| {

        b.iter_batched(
            || {
                let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, DEFAULT_BLOCK_CONCURRENCY);
                let nezha = _get_nezha_executor();
                let SimulationResult { rw_sets, .. } = nezha._simulate(consensus_output);
                rw_sets
            },
            |rw_sets| {
                let _ = AddressBasedConflictGraph::construct(rw_sets)
                    .hierarchcial_sort()
                    .reorder()
                    .extract_schedule();
            },
            BatchSize::SmallInput
        );
    });
}

fn commit(c: &mut Criterion) {
    c.bench_function("commit", |b| {

        b.iter_batched(
            || {
                let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, DEFAULT_BLOCK_CONCURRENCY);
                let nezha = _get_nezha_executor();
                let SimulationResult { rw_sets, .. } = nezha._simulate(consensus_output);
                let scheduled_info = AddressBasedConflictGraph::construct(rw_sets)
                    .hierarchcial_sort()
                    .reorder()
                    .extract_schedule();
                (nezha, scheduled_info)
            },
            |(nezha, scheduled_info)| {
                nezha._concurrent_commit(scheduled_info, 1);
            },
            BatchSize::SmallInput
        );
    });
}

fn chunk_size(c: &mut Criterion) {
    let chunk_sizes = (2..40 as usize).step_by(2);
    let mut group = c.benchmark_group("Nezha Benchmark according to chunksize at block concurrency 13");
    group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*13) as u64));
    for chuck_size in chunk_sizes {
        group.bench_with_input(
            criterion::BenchmarkId::new("nezha-chunksize", chuck_size),
            &chuck_size,
            |b, chuck_size| {
                b.iter_batched(
                    || {
                        let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, 13);
                        let nezha = _get_nezha_executor();
                        (nezha, consensus_output)
                    },
                    |(nezha, consensus_output)| {
                        let SimulationResult { rw_sets, .. } = nezha._simulate(consensus_output);
                        let scheduled_info = AddressBasedConflictGraph::construct(rw_sets)
                            .hierarchcial_sort()
                            .reorder()
                            .extract_schedule();
                        nezha._concurrent_commit(scheduled_info, *chuck_size)
                    },
                    BatchSize::SmallInput
                );
            }
        );
    }
}

// criterion_group!(benches, simulation, nezha, commit, block_concurrency);
// criterion_group!(benches, block_concurrency_simulation);
criterion_group!(benches, block_concurrency_simulation, block_concurrency_scheduling, block_concurrency_commit);
criterion_main!(benches);