#![allow(dead_code)]
use criterion::Throughput;
use criterion::{criterion_group, criterion_main, Criterion, BatchSize};
use ethers_providers::{Provider, MockProvider};

use parking_lot::RwLock;
use sslab_execution::{
    types::ExecutableEthereumBatch,
    utils::smallbank_contract_benchmark::concurrent_evm_storage,
    utils::test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID}
};

use sslab_execution_nezha::{
    AddressBasedConflictGraph,
    ConcurrencyLevelManager,
    SimulationResult, SimulatedTransaction,
};

const DEFAULT_BATCH_SIZE: usize = 200;
const DEFAULT_BLOCK_CONCURRENCY: usize = 12;
const DEFAULT_SKEWNESS: f32 = 0.0;

fn _get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn _get_nezha_executor(clevel: usize) -> ConcurrencyLevelManager {
    ConcurrencyLevelManager::new(concurrent_evm_storage(), clevel)
}

fn _create_random_smallbank_workload(skewness: f32, batch_size: usize, block_concurrency: usize) -> Vec<ExecutableEthereumBatch> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, 100_000)
}

fn _get_rw_sets(nezha: std::sync::Arc<ConcurrencyLevelManager>, consensus_output: Vec<ExecutableEthereumBatch>) -> Vec<SimulatedTransaction> {
    let (tx, rx) = std::sync::mpsc::channel();
    let _ = tokio::runtime::Handle::current().spawn(async move {
        let SimulationResult { rw_sets, .. } = nezha.simulate(consensus_output).await;
        tx.send(rw_sets).unwrap();
    });
    rx.recv().unwrap()
}
fn block_concurrency_no_abort(c: &mut Criterion) {
    let param = 1..81;
    let mut group = c.benchmark_group("Nezha No Abort Benchmark according to block concurrency");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("nezha", i),
            &i,
            |b, i| {
                b.to_async(tokio::runtime::Runtime::new().unwrap()).iter_batched(
                    || {
                        let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
                        let nezha = _get_nezha_executor(*i);
                        (nezha, consensus_output)
                    },
                    |(nezha, consensus_output)| async move {

                        nezha._execute(consensus_output).await
                    },
                    BatchSize::SmallInput
                );
            }
        );
    }
}

fn block_concurrency(c: &mut Criterion) {
    let param = 1..81;
    let mut group = c.benchmark_group("Nezha Benchmark according to block concurrency");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
        let effective_tps: std::rc::Rc<RwLock<Vec<(_,_)>>> =  std::rc::Rc::new(RwLock::new(Vec::new()));
        let duration_metrics = std::sync::Arc::new(RwLock::new(Vec::new()));
        group.bench_with_input(
            criterion::BenchmarkId::new("nezha", i),
            &(i, duration_metrics.clone(), effective_tps.clone()),
            |b, (i, duration_metrics, effective_tps)| {
                b.to_async(tokio::runtime::Runtime::new().unwrap()).iter_batched(
                    || {
                        let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
                        let nezha = _get_nezha_executor(*i);
                        (nezha, consensus_output)
                    },
                    |(nezha, consensus_output)| async move {

                        let now = tokio::time::Instant::now();
                        let SimulationResult { rw_sets, .. } = nezha.simulate(consensus_output).await;
                        let simulation = now.elapsed().as_micros() as f64/1000f64;

                        let now = tokio::time::Instant::now();
                        let (scheduled_info, _) = AddressBasedConflictGraph::par_construct(rw_sets).await
                            .hierarchcial_sort()
                            .reorder()
                            .par_extract_schedule().await;
                        let scheduling = now.elapsed().as_micros() as f64/1000f64;

                        effective_tps.write().push((scheduled_info.scheduled_txs_len(), scheduled_info.aborted_txs_len()+scheduled_info.scheduled_txs_len()));

                        let now = tokio::time::Instant::now();
                        nezha._concurrent_commit(scheduled_info, 1).await;
                        let commit = now.elapsed().as_micros() as f64/1000f64;

                        duration_metrics.write().push((simulation, scheduling, commit));
                    },
                    BatchSize::SmallInput
                );
            }
        );

        let (mut simulation, mut scheduling, mut commit) = (0 as f64, 0 as f64,0 as f64);
        let len = duration_metrics.read().len() as f64;

        for (a1, a2, a3) in duration_metrics.read().iter() {
            simulation += a1;
            scheduling += a2;
            commit += a3;
        };

        println!("Simulation: {:.4}", simulation/len);
        println!("Scheduling: {:.4}", scheduling/len);
        println!("Commit: {:.4}", commit/len);

        let (mut committed, mut total) = (0, 0);
        let len = effective_tps.read().len();
        for (a, b) in effective_tps.read().iter() {
            committed += a;
            total += b;
        }
        println!("committed: {:?} / total: {:?}", committed as f64/ len as f64, total as f64/ len as f64);
    }
}

fn block_concurrency_simulation(c: &mut Criterion) {

    let mut group = c.benchmark_group("simulation according to block concurrency");
    let param = 41..81;
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("nezha", i),
            &i,
            |b, i| {
                b.to_async(tokio::runtime::Runtime::new().unwrap()).iter_batched(
                    || {
                        let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
                        let nezha = _get_nezha_executor(*i);
                        (nezha, consensus_output)
                    },
                    |(nezha, consensus_output)| async move {
                        let _ = nezha.simulate(consensus_output).await;
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
    let param =1..81;
    let mut group = c.benchmark_group("scheduling according to block concurrency");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
        // let parallelism_metrics: std::rc::Rc<RwLock<Vec<(_, _, _, _, _)>>> =  std::rc::Rc::new(RwLock::new(Vec::new()));
        let duration_metrics = std::sync::Arc::new(RwLock::new(Vec::new()));
        group.bench_with_input(
            criterion::BenchmarkId::new("nezha", i),
            &(i, duration_metrics.clone()),
            |b, (i, metrics)| {
                b.to_async(tokio::runtime::Runtime::new().unwrap()).iter_batched(
                    || {
                        let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
                        let nezha = std::sync::Arc::new(_get_nezha_executor(*i));
                        let rw_sets = _get_rw_sets(nezha.clone(), consensus_output.clone());
                        rw_sets
                    },
                    |rw_sets| async move {
                        let now = tokio::time::Instant::now();
                        let mut acg = AddressBasedConflictGraph::par_construct(rw_sets).await;
                        let construction = now.elapsed().as_micros() as f64/1000f64;
                        

                        let now = tokio::time::Instant::now();
                        acg.hierarchcial_sort();
                        let sorting = now.elapsed().as_micros() as f64/1000f64;
                        

                        let now = tokio::time::Instant::now();
                        acg.reorder();
                        let reordering = now.elapsed().as_micros() as f64/1000f64;
                        

                        let now = tokio::time::Instant::now();
                        let _result = acg.par_extract_schedule().await;
                        let extraction = now.elapsed().as_micros() as f64/1000f64;
                        
                        metrics.write().push((construction, sorting, reordering, extraction))
                        // metrics.write().unwrap().push(result.parallism_metric())
                    },
                    BatchSize::SmallInput
                );
            }
        );

        let (mut construction, mut sorting, mut reordering, mut extraction) = (0 as f64, 0 as f64,0 as f64, 0 as f64);
        let len = duration_metrics.read().len() as f64;

        for (a1, a2, a3, a4) in duration_metrics.read().iter() {
            construction += a1;
            sorting += a2;
            reordering += a3;
            extraction += a4;
        };

        println!("ACG construct: {:.4}", construction/len);
        println!("Hierachical sort: {:.4}", sorting/len);
        println!("Reorder: {:.4}", reordering/len);
        println!("Extract schedule: {:.4}",extraction/len);


        // let (mut total_tx, mut average_width, mut std_width, mut max_width, mut depth) = (0 as usize, 0 as f64, 0 as f64, 0 as usize, 0 as usize);
        // let len = parallelism_metrics.read().unwrap().len();
        // for (a1, a2, a3, a4, a5) in parallelism_metrics.read().unwrap().iter() {
        //     total_tx += a1;
        //     average_width += a2;
        //     std_width += a3;
        //     max_width += a4;
        //     depth += a5;
        // };
        // println!("total_tx: {}, average_width: {:.2}, std_width: {:.2} max_width: {:.2}, depth: {:.2}", total_tx/len, average_width/len as f64, std_width/len as f64, max_width as f64/len as f64, depth as f64/len as f64)
    
    
    }
}

fn block_concurrency_commit(c: &mut Criterion) {
    let param = 41..81;
    let mut group = c.benchmark_group("concurrent commit according to block concurrency");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("nezha", i),
            &i,
            |b, i| {
                b.to_async(tokio::runtime::Runtime::new().unwrap()).iter_batched(
                    || {
                        let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
                        let nezha = std::sync::Arc::new(_get_nezha_executor(*i));
                        let rw_sets = _get_rw_sets(nezha.clone(), consensus_output.clone());
                        let scheduled_info = AddressBasedConflictGraph::construct(rw_sets)
                            .hierarchcial_sort()
                            .reorder()
                            .extract_schedule();
                        (nezha, scheduled_info)
                    },
                    |(nezha, scheduled_info)| async move {
                        nezha._concurrent_commit(scheduled_info, 1).await;
                    },
                    BatchSize::SmallInput
                );
            }
        );
    }
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
                b.to_async(tokio::runtime::Runtime::new().unwrap()).iter_batched(
                    || {
                        let consensus_output = _create_random_smallbank_workload(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, 13);
                        let nezha = _get_nezha_executor(13);
                        (nezha, consensus_output)
                    },
                    |(nezha, consensus_output)| async move {
                        let SimulationResult { rw_sets, .. } = nezha.simulate(consensus_output).await;
                        let scheduled_info = AddressBasedConflictGraph::construct(rw_sets)
                            .hierarchcial_sort()
                            .reorder()
                            .extract_schedule();
                        nezha._concurrent_commit(scheduled_info, *chuck_size).await
                    },
                    BatchSize::SmallInput
                );
            }
        );
    }
}

criterion_group!(benches, block_concurrency_no_abort);
// criterion_group!(benches, block_concurrency_simulation, block_concurrency_commit);
criterion_main!(benches);
