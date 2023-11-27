use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion, BatchSize};
use ethers_providers::{Provider, MockProvider};
use narwhal_types::BatchDigest;
use parking_lot::RwLock;

use sslab_core::{
    types::ExecutableEthereumBatch,
    utils::smallbank_contract_benchmark::default_memory_storage,
    execution_models::nezha::{
        AddressBasedConflictGraph,
        Nezha,
        SimulationResult,
        tests::utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID}
    },
};

const DEFAULT_BATCH_SIZE: i32 = 200;
const DEFAULT_BLOCK_CONCURRENCY: i32 = 12;
const DEFAULT_SKEWNESS: f32 = 0.0;

fn _get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn _get_nezha_executor() -> Nezha {
    let memory_storage = Arc::new(RwLock::new(default_memory_storage()));
    Nezha::new(memory_storage)
}

fn _create_random_smallbank_workload(skewness: f32, batch_size: i32, block_concurrency: i32) -> Vec<ExecutableEthereumBatch> {
    let handler = _get_smallbank_handler();

    let mut consensus_output = Vec::new();
    for _ in 0..block_concurrency {
        let mut tmp = Vec::new();
        for _ in 0..batch_size {
            tmp.push(handler.random_operation(skewness, 10_000))
        }
        consensus_output.push(ExecutableEthereumBatch::new(tmp, BatchDigest::default()));
    }

    consensus_output
}

fn block_concurrency(c: &mut Criterion) {
    let param = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20];
    let mut group = c.benchmark_group("Nezha Benchmark according to block concurrency");
    
    for i in param.iter() {
        group.bench_with_input(
            criterion::BenchmarkId::new("nezha", i),
            i,
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
                        nezha._concurrent_commit(scheduled_info)
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
                nezha._concurrent_commit(scheduled_info);
            },
            BatchSize::SmallInput
        );
    });
}

criterion_group!(benches, simulation, nezha, commit, block_concurrency);
criterion_main!(benches);