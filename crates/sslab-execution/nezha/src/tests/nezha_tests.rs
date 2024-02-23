use ethers_core::types::{H160, H256, transaction::eip2718::TypedTransaction};
use evm::executor::stack::{RwSet, Simulatable};
use hashbrown::HashSet;
use sslab_execution::types::EthereumTransaction;

use crate::{types::SimulatedTransaction, address_based_conflict_graph::AddressBasedConflictGraph};

const CONTRACT_ADDR: u64 = 0x1;

fn transaction_with_rw(tx_id: u64, read_addr: u64, write_addr: u64) -> SimulatedTransaction {
    let mut set = RwSet::new();
    set.record_read_key(
        H160::from_low_u64_be(CONTRACT_ADDR), 
        H256::from_low_u64_be(read_addr), 
        H256::from_low_u64_be(1));
    set.record_write_key(
        H160::from_low_u64_be(CONTRACT_ADDR), 
        H256::from_low_u64_be(write_addr), 
        H256::from_low_u64_be(1));
    SimulatedTransaction::new(H256::from_low_u64_be(tx_id), Some(set), Vec::new(), Vec::new(), EthereumTransaction(TypedTransaction::default()))
}

fn transaction_with_multiple_rw(tx_id: u64, read_addr: Vec<u64>, write_addr: Vec<u64>) -> SimulatedTransaction {
    let mut set = RwSet::new();
    read_addr.iter().for_each(|addr| {
        set.record_read_key(
            H160::from_low_u64_be(CONTRACT_ADDR), 
            H256::from_low_u64_be(*addr), 
            H256::from_low_u64_be(1));
    });
    write_addr.iter().for_each(|addr| {
        set.record_write_key(
            H160::from_low_u64_be(CONTRACT_ADDR), 
            H256::from_low_u64_be(*addr), 
            H256::from_low_u64_be(1));
    });
    SimulatedTransaction::new(H256::from_low_u64_be(tx_id), Some(set), Vec::new(), Vec::new(), EthereumTransaction(TypedTransaction::default()))
}

fn nezha_test(input_txs: Vec<SimulatedTransaction>, answer: Vec<Vec<u64>>, print_result: bool) {
    let scheduled_info = AddressBasedConflictGraph::construct(input_txs.clone())
            .hierarchcial_sort()
            .reorder()
            .extract_schedule();
    
    if print_result {
        println!("Scheduled Transactions:");
        scheduled_info.scheduled_txs.iter()
            .for_each(|txs| {
                txs.iter().for_each(|tx| {
                    print!("{} ", tx.id().to_low_u64_be());
                });
                print!("\n");
            });
    }
    
    scheduled_info.scheduled_txs.iter().zip(answer.iter()).for_each(|(txs, idx)| {
        assert_eq!(txs.len(), idx.len());
        let answer_set:HashSet<&u64> = idx.iter().collect();
        assert!(txs.iter().all(|tx| answer_set.contains(&tx.id().to_low_u64_be())))
     });
}

async fn nezha_par_test(input_txs: Vec<SimulatedTransaction>, answer: Vec<Vec<u64>>, print_result: bool) {
    let scheduled_info = AddressBasedConflictGraph::par_construct(input_txs.clone()).await
            .hierarchcial_sort()
            .reorder()
            .par_extract_schedule().await;
    
    if print_result {
        println!("Scheduled Transactions:");
        scheduled_info.scheduled_txs.iter()
            .for_each(|txs| {
                txs.iter().for_each(|tx| {
                    print!("{} ", tx.id().to_low_u64_be());
                });
                print!("\n");
            });
    }
    
    scheduled_info.scheduled_txs.iter().zip(answer.iter()).for_each(|(txs, idx)| {
        assert_eq!(txs.len(), idx.len());
        let answer_set:HashSet<&u64> = idx.iter().collect();
        assert!(txs.iter().all(|tx| answer_set.contains(&tx.id().to_low_u64_be())))
     });
}

#[tokio::test]
async fn test_scenario_1() {
    let txs = vec![
        transaction_with_rw(1, 2, 1),
        transaction_with_rw(2, 3, 2),
        transaction_with_rw(3, 4, 2),
        transaction_with_rw(4, 4, 3),
        transaction_with_rw(5, 4, 4),
        transaction_with_rw(6, 1, 3)
    ];

    let answer = vec![
        vec![2],
        vec![3, 4],
        vec![5, 6]
    ];

    nezha_test(txs.clone(), answer.clone(), false);
    nezha_par_test(txs, answer, false).await;
}

#[tokio::test]
async fn test_scenario_2() {
    let txs = vec![
        transaction_with_rw(1, 2, 1),
        transaction_with_rw(3, 4, 2),
        transaction_with_rw(2, 3, 2),
        transaction_with_rw(4, 4, 3),
        transaction_with_rw(5, 4, 4),
        transaction_with_rw(6, 1, 3)
    ];
    
    let answer = vec![
        vec![3],
        vec![2],
        vec![4],
        vec![5, 6],
    ];
    
    nezha_test(txs.clone(), answer.clone(), false);
    nezha_par_test(txs, answer, false).await;
}

#[tokio::test]
async fn test_scenario_3() {
    let txs = vec![
        transaction_with_rw(1, 2, 1),
        transaction_with_rw(2, 3, 2),
        transaction_with_rw(3, 4, 2),
        transaction_with_rw(6, 1, 3),
        transaction_with_rw(5, 4, 4),
        transaction_with_rw(4, 4, 3),
    ];
    
    let answer = vec![
        vec![2],
        vec![3, 6],
        vec![4],
        vec![5],
    ];
    
    nezha_test(txs.clone(), answer.clone(), false);
    nezha_par_test(txs, answer, false).await;
}

#[tokio::test]
async fn test_scenario_4() {
    let txs = vec![
        transaction_with_rw(1, 2, 1),
        transaction_with_rw(2, 3, 2),
        transaction_with_rw(3, 4, 2),
        transaction_with_rw(4, 4, 4),
        transaction_with_rw(5, 4, 4),
        transaction_with_rw(6, 1, 3),
    ];
    
    let answer = vec![
        vec![1],
        vec![2],
        vec![3],
        vec![4],
    ];
    
    nezha_test(txs.clone(), answer.clone(), false);
    nezha_par_test(txs, answer, false).await;
}

#[tokio::test]
async fn test_scenario_5() {
    let txs = vec![
        transaction_with_rw(1, 2, 1),
        transaction_with_rw(2, 3, 2),
        transaction_with_rw(3, 4, 2),
        transaction_with_rw(4, 4, 4),
        transaction_with_rw(5, 4, 4),
        transaction_with_rw(6, 1, 3),
        transaction_with_rw(7, 4, 4),
    ];
    
    let answer = vec![
        vec![1],
        vec![2],
        vec![3],
        vec![4],
    ];
    
    nezha_test(txs.clone(), answer.clone(), false);
    nezha_par_test(txs, answer, false).await;
}

#[tokio::test]
async fn test_reordering() {
    let txs = vec![
        transaction_with_multiple_rw(1, vec![], vec![1, 2]),
        transaction_with_rw(2, 2, 1),
    ];
    
    let answer = vec![
        vec![2],
        vec![1],
    ];
    
    nezha_test(txs.clone(), answer.clone(), false);
    nezha_par_test(txs, answer, false).await;
}