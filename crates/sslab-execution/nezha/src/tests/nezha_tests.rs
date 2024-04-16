use std::str::FromStr;

use ethers_core::types::{H160, H256};
use evm::executor::stack::{RwSet, Simulatable};
use hashbrown::HashSet;
use itertools::Itertools as _;
use sslab_execution::types::{EthereumTransaction, IndexedEthereumTransaction};

use crate::{
    address_based_conflict_graph::{AddressBasedConflictGraph, Transaction},
    types::SimulatedTransaction,
};

const CONTRACT_ADDR: u64 = 0x1;

fn transaction_with_rw(tx_id: u64, read_addr: u64, write_addr: u64) -> SimulatedTransaction {
    let mut set = RwSet::new();
    set.record_read_key(
        H160::from_low_u64_be(CONTRACT_ADDR),
        H256::from_low_u64_be(read_addr),
        H256::from_low_u64_be(1),
    );
    set.record_write_key(
        H160::from_low_u64_be(CONTRACT_ADDR),
        H256::from_low_u64_be(write_addr),
        H256::from_low_u64_be(1),
    );
    SimulatedTransaction::new(
        Some(set),
        Vec::new(),
        Vec::new(),
        IndexedEthereumTransaction::new(EthereumTransaction::default(), tx_id),
    )
}

fn transaction_with_multiple_rw(
    tx_id: u64,
    read_addr: Vec<u64>,
    write_addr: Vec<u64>,
) -> SimulatedTransaction {
    let mut set = RwSet::new();
    read_addr.iter().for_each(|addr| {
        set.record_read_key(
            H160::from_low_u64_be(CONTRACT_ADDR),
            H256::from_low_u64_be(*addr),
            H256::from_low_u64_be(1),
        );
    });
    write_addr.iter().for_each(|addr| {
        set.record_write_key(
            H160::from_low_u64_be(CONTRACT_ADDR),
            H256::from_low_u64_be(*addr),
            H256::from_low_u64_be(1),
        );
    });
    SimulatedTransaction::new(
        Some(set),
        Vec::new(),
        Vec::new(),
        IndexedEthereumTransaction::new(EthereumTransaction::default(), tx_id),
    )
}

fn transaction_with_multiple_rw_str(
    tx_id: u64,
    read_addr: Vec<&str>,
    write_addr: Vec<&str>,
) -> SimulatedTransaction {
    let mut set = RwSet::new();
    read_addr.into_iter().for_each(|addr| {
        set.record_read_key(
            H160::from_low_u64_be(CONTRACT_ADDR),
            H256::from_str(addr).unwrap(),
            H256::from_low_u64_be(1),
        );
    });
    write_addr.into_iter().for_each(|addr| {
        set.record_write_key(
            H160::from_low_u64_be(CONTRACT_ADDR),
            H256::from_str(addr).unwrap(),
            H256::from_low_u64_be(1),
        );
    });
    SimulatedTransaction::new(
        Some(set),
        Vec::new(),
        Vec::new(),
        IndexedEthereumTransaction::new(EthereumTransaction::default(), tx_id),
    )
}

fn nezha_test(input_txs: Vec<SimulatedTransaction>, answer: Vec<Vec<u64>>, print_result: bool) {
    let scheduled_info = AddressBasedConflictGraph::construct(input_txs.clone())
        .hierarchcial_sort()
        .reorder()
        .extract_schedule();

    if print_result {
        println!("Scheduled Transactions:");
        scheduled_info.scheduled_txs.iter().for_each(|txs| {
            txs.iter().for_each(|tx| {
                print!("{} ", tx.id());
            });
            print!("\n");
        });

        println!("Aborted Transactions:");
        scheduled_info.aborted_txs.iter().for_each(|tx| {
            print!("{}\n", tx.id());
        });
    }

    scheduled_info
        .scheduled_txs
        .iter()
        .zip(answer.iter())
        .for_each(|(txs, idx)| {
            assert_eq!(txs.len(), idx.len());
            let answer_set: HashSet<&u64> = idx.iter().collect();
            assert!(txs.iter().all(|tx| answer_set.contains(&tx.id())))
        });

    let aborted_tx_len = input_txs.len() - answer.iter().flatten().count();
    assert_eq!(aborted_tx_len, scheduled_info.aborted_txs_len());

    scheduled_info.aborted_txs.into_iter().for_each(|tx| {
        std::sync::Arc::try_unwrap(tx).unwrap();
    })
}

async fn nezha_par_test(
    input_txs: Vec<SimulatedTransaction>,
    answer: Vec<Vec<u64>>,
    print_result: bool,
) {
    let scheduled_info = AddressBasedConflictGraph::par_construct(input_txs.clone())
        .await
        .hierarchcial_sort()
        .reorder()
        .par_extract_schedule()
        .await;

    if print_result {
        println!("Scheduled Transactions:");
        scheduled_info.scheduled_txs.iter().for_each(|txs| {
            txs.iter().for_each(|tx| {
                print!("{} ", tx.id());
            });
            print!("\n");
        });
    }

    scheduled_info
        .scheduled_txs
        .iter()
        .zip(answer.iter())
        .for_each(|(txs, idx)| {
            assert_eq!(txs.len(), idx.len());
            let answer_set: HashSet<&u64> = idx.iter().collect();
            assert!(txs.iter().all(|tx| answer_set.contains(&tx.id())))
        });

    let aborted_tx_len = input_txs.len() - answer.iter().flatten().count();
    assert_eq!(aborted_tx_len, scheduled_info.aborted_txs_len());

    scheduled_info.aborted_txs.into_iter().for_each(|tx| {
        std::sync::Arc::try_unwrap(tx).unwrap();
    })
}
fn optimistic_nezha_test(
    input_txs: Vec<SimulatedTransaction>,
    answer: Vec<Vec<u64>>,
    print_result: bool,
) {
    let scheduled_info = AddressBasedConflictGraph::optimistic_construct(
        input_txs
            .iter()
            .map(|tx| std::sync::Arc::new(Transaction::from(tx.clone()).0))
            .collect_vec(),
    )
    .hierarchcial_sort()
    .reorder()
    .extract_schedule();

    if print_result {
        println!("Scheduled Transactions:");
        scheduled_info.scheduled_txs.iter().for_each(|txs| {
            txs.iter().for_each(|tx| {
                print!("{} ", tx.id());
            });
            print!("\n");
        });

        println!("Aborted Transactions:");
        scheduled_info.aborted_txs.iter().for_each(|tx| {
            print!("{}\n", tx.id());
        });
    }

    scheduled_info
        .scheduled_txs
        .iter()
        .zip(answer.iter())
        .for_each(|(txs, idx)| {
            assert_eq!(txs.len(), idx.len());
            let answer_set: HashSet<&u64> = idx.iter().collect();
            assert!(txs.iter().all(|tx| answer_set.contains(&tx.id())))
        });

    let aborted_tx_len = input_txs.len() - answer.iter().flatten().count();
    assert_eq!(aborted_tx_len, scheduled_info.aborted_txs_len());

    scheduled_info.aborted_txs.into_iter().for_each(|tx| {
        std::sync::Arc::try_unwrap(tx).unwrap();
    })
}

async fn optimistic_nezha_par_test(
    input_txs: Vec<SimulatedTransaction>,
    answer: Vec<Vec<u64>>,
    print_result: bool,
) {
    let scheduled_info = AddressBasedConflictGraph::par_optimistic_construct(
        input_txs
            .iter()
            .map(|tx| std::sync::Arc::new(Transaction::from(tx.clone()).0))
            .collect_vec(),
    )
    .await
    .hierarchcial_sort()
    .reorder()
    .par_extract_schedule()
    .await;

    if print_result {
        println!("Scheduled Transactions:");
        scheduled_info.scheduled_txs.iter().for_each(|txs| {
            txs.iter().for_each(|tx| {
                print!("{} ", tx.id());
            });
            print!("\n");
        });

        println!("Aborted Transactions:");
        scheduled_info.aborted_txs.iter().for_each(|tx| {
            print!("{}\n", tx.id());
        });
    }

    scheduled_info
        .scheduled_txs
        .iter()
        .zip(answer.iter())
        .for_each(|(txs, idx)| {
            assert_eq!(txs.len(), idx.len());
            let answer_set: HashSet<&u64> = idx.iter().collect();
            assert!(txs.iter().all(|tx| answer_set.contains(&tx.id())))
        });

    let aborted_tx_len = input_txs.len() - answer.iter().flatten().count();
    assert_eq!(aborted_tx_len, scheduled_info.aborted_txs_len());

    scheduled_info.aborted_txs.into_iter().for_each(|tx| {
        std::sync::Arc::try_unwrap(tx).unwrap();
    })
}

#[tokio::test]
async fn test_scenario_1() {
    let txs = vec![
        transaction_with_rw(1, 2, 1),
        transaction_with_rw(2, 3, 2),
        transaction_with_rw(3, 4, 2),
        transaction_with_rw(4, 4, 3),
        transaction_with_rw(5, 4, 4),
        transaction_with_rw(6, 1, 3),
    ];

    let answer = vec![vec![2], vec![3, 4], vec![5, 6]];

    nezha_test(txs.clone(), answer.clone(), false);
    nezha_par_test(txs.clone(), answer.clone(), false).await;
    optimistic_nezha_test(txs.clone(), answer.clone(), false);
    optimistic_nezha_par_test(txs, answer, false).await;
}

#[tokio::test]
async fn test_scenario_2() {
    let txs = vec![
        transaction_with_rw(1, 2, 1),
        transaction_with_rw(3, 4, 2),
        transaction_with_rw(2, 3, 2),
        transaction_with_rw(4, 4, 3),
        transaction_with_rw(5, 4, 4),
        transaction_with_rw(6, 1, 3),
    ];

    let answer = vec![vec![3], vec![2], vec![4], vec![5, 6]];

    nezha_test(txs.clone(), answer.clone(), false);
    nezha_par_test(txs.clone(), answer.clone(), false).await;
    optimistic_nezha_test(txs.clone(), answer.clone(), false);
    optimistic_nezha_par_test(txs, answer, false).await;
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

    let answer = vec![vec![2], vec![3, 6], vec![4], vec![5]];

    nezha_test(txs.clone(), answer.clone(), false);
    nezha_par_test(txs.clone(), answer.clone(), false).await;
    optimistic_nezha_test(txs.clone(), answer.clone(), false);
    optimistic_nezha_par_test(txs, answer, false).await;
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

    let answer = vec![vec![1], vec![2], vec![3], vec![4]];

    nezha_test(txs.clone(), answer.clone(), false);
    nezha_par_test(txs.clone(), answer.clone(), false).await;
    optimistic_nezha_test(txs.clone(), answer.clone(), false);
    optimistic_nezha_par_test(txs, answer, false).await;
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

    let answer = vec![vec![1], vec![2], vec![3], vec![4]];

    nezha_test(txs.clone(), answer.clone(), false);
    nezha_par_test(txs.clone(), answer.clone(), false).await;
    optimistic_nezha_test(txs.clone(), answer.clone(), false);
    optimistic_nezha_par_test(txs, answer, false).await;
}

#[tokio::test]
async fn test_reordering() {
    let txs = vec![
        transaction_with_multiple_rw(1, vec![], vec![1, 2]),
        transaction_with_rw(2, 2, 1),
    ];

    let answer = vec![vec![2], vec![1]];

    nezha_test(txs.clone(), answer.clone(), false);
    nezha_par_test(txs.clone(), answer.clone(), false).await;
    optimistic_nezha_test(txs.clone(), answer.clone(), false);
    optimistic_nezha_par_test(txs, answer, false).await;
}

#[tokio::test]
async fn test_scenario_6() {
    let txs = vec![
        transaction_with_multiple_rw_str(
            1,
            vec![
                "0x48c8d13a49dbf1c93484ba997be20d9cae319d82960232db3544bb8bf65d4ac0",
                "0xe3ea58be4f1efa6db4e24abc274fb1bccd82dfcd49c8f508a08c911f0357c19d",
            ],
            vec![
                "0x48c8d13a49dbf1c93484ba997be20d9cae319d82960232db3544bb8bf65d4ac0",
                "0xe3ea58be4f1efa6db4e24abc274fb1bccd82dfcd49c8f508a08c911f0357c19d",
            ],
        ),
        transaction_with_multiple_rw_str(
            2,
            vec![
                "0x7b6a909101d770fd973075a9dbcef6c7ae894d77f3f89dcacb997ab3178cd44e",
                "0xb955ea50cf68e45358af8183015c9694f0e9401fee45e367d90c462108f102bd",
            ],
            vec![
                "0x7b6a909101d770fd973075a9dbcef6c7ae894d77f3f89dcacb997ab3178cd44e",
                "0xb955ea50cf68e45358af8183015c9694f0e9401fee45e367d90c462108f102bd",
            ],
        ),
        transaction_with_multiple_rw_str(
            3,
            vec![
                "0x7b6a909101d770fd973075a9dbcef6c7ae894d77f3f89dcacb997ab3178cd44e",
                "0xe3ea58be4f1efa6db4e24abc274fb1bccd82dfcd49c8f508a08c911f0357c19d",
            ],
            vec![
                "0x7b6a909101d770fd973075a9dbcef6c7ae894d77f3f89dcacb997ab3178cd44e",
                "0xe3ea58be4f1efa6db4e24abc274fb1bccd82dfcd49c8f508a08c911f0357c19d",
            ],
        ),
    ];

    let answer = vec![vec![1, 2]];

    nezha_test(txs.clone(), answer.clone(), false);
    nezha_par_test(txs.clone(), answer.clone(), false).await;
    optimistic_nezha_test(txs.clone(), answer.clone(), false);
    optimistic_nezha_par_test(txs, answer, false).await;
}
