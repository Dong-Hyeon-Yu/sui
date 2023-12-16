#![allow(dead_code)]
use std::sync::RwLock;

use criterion::Throughput;
use criterion::{criterion_group, criterion_main, Criterion, BatchSize};
use ethers_providers::{Provider, MockProvider};

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

const DEFAULT_BATCH_SIZE: usize = 200;
const DEFAULT_BLOCK_CONCURRENCY: usize = 12;
const DEFAULT_SKEWNESS: f32 = 1.0;

fn _get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn _get_nezha_executor(clevel: usize) -> ConcurrencyLevelManager {
    ConcurrencyLevelManager::new(concurrent_evm_storage(), clevel)
}

fn _create_random_smallbank_workload(skewness: f32, batch_size: usize, block_concurrency: usize) -> Vec<ExecutableEthereumBatch> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, 10_000)
}

fn block_concurrency(c: &mut Criterion) {
    let param = 1..41;
    let mut group = c.benchmark_group("Nezha Benchmark according to block concurrency");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
        // let effective_tps: std::rc::Rc<RwLock<Vec<(_,_)>>> =  std::rc::Rc::new(RwLock::new(Vec::new()));
        group.bench_with_input(
            criterion::BenchmarkId::new("nezha", i),
            &i,
            |b, i| {
                b.iter_batched(
                    || {
                        let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
                        let nezha = _get_nezha_executor(*i);
                        (nezha, consensus_output)
                    },
                    |(nezha, consensus_output)| {
                        let SimulationResult { rw_sets, .. } = nezha._simulate(consensus_output);
                        let scheduled_info = AddressBasedConflictGraph::construct(rw_sets)
                            .hierarchcial_sort()
                            .reorder()
                            .extract_schedule();
                        // effective_tps.write().unwrap().push((scheduled_info.scheduled_txs_len(), scheduled_info.aborted_txs_len()+scheduled_info.scheduled_txs_len()));
                        nezha._concurrent_commit(scheduled_info, 1)
                    },
                    BatchSize::SmallInput
                );
            }
        );
        // let (mut committed, mut total) = (0, 0);
        // let len = effective_tps.read().unwrap().len();
        // for (a, b) in effective_tps.read().unwrap().iter() {
        //     committed += a;
        //     total += b;
        // }
        // println!("committed: {:?} / total: {:?}", committed as f64/ len as f64, total as f64/ len as f64);
    }
}

fn block_concurrency_simulation(c: &mut Criterion) {

    let mut group = c.benchmark_group("simulation according to block concurrency");
    let param = 1..41;
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("nezha", i),
            &i,
            |b, i| {
                b.iter_batched(
                    || {
                        let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
                        let nezha = _get_nezha_executor(*i);
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
        let parallelism_metrics: std::rc::Rc<RwLock<Vec<(_, _, _, _, _)>>> =  std::rc::Rc::new(RwLock::new(Vec::new()));
        group.bench_with_input(
            criterion::BenchmarkId::new("nezha", i),
            &(i, parallelism_metrics.clone()),
            |b, (i, metrics)| {
                b.iter_batched(
                    || {
                        let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
                        let nezha = _get_nezha_executor(*i);
                        let SimulationResult { rw_sets, .. } = nezha._simulate(consensus_output);
                        rw_sets
                    },
                    |rw_sets| {
                        let result = AddressBasedConflictGraph::construct(rw_sets)
                            .hierarchcial_sort()
                            .reorder()
                            .extract_schedule();
                        metrics.write().unwrap().push(result.parallism_metric())
                    },
                    BatchSize::SmallInput
                );
            }
        );

        let (mut total_tx, mut average_width, mut std_width, mut max_width, mut depth) = (0 as usize, 0 as f64, 0 as f64, 0 as usize, 0 as usize);
        let len = parallelism_metrics.read().unwrap().len();
        for (a1, a2, a3, a4, a5) in parallelism_metrics.read().unwrap().iter() {
            total_tx += a1;
            average_width += a2;
            std_width += a3;
            max_width += a4;
            depth += a5;
        };
        println!("total_tx: {}, average_width: {:.2}, std_width: {:.2} max_width: {:.2}, depth: {:.2}", total_tx/len, average_width/len as f64, std_width/len as f64, max_width as f64/len as f64, depth as f64/len as f64)
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
                        let nezha = _get_nezha_executor(*i);
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
                let nezha = _get_nezha_executor(DEFAULT_BLOCK_CONCURRENCY);
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
                let nezha = _get_nezha_executor(DEFAULT_BLOCK_CONCURRENCY);
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
                let nezha = _get_nezha_executor(DEFAULT_BLOCK_CONCURRENCY);
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
                        let nezha = _get_nezha_executor(13);
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
criterion_group!(benches, block_concurrency);
// criterion_group!(benches, block_concurrency_simulation, block_concurrency_scheduling, block_concurrency_commit);
criterion_main!(benches);
