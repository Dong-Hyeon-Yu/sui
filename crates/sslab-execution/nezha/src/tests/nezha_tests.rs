use std::str::FromStr;

use reth::revm::primitives::{ExecutionResult, HashMap, HashSet};
use reth::{primitives::TransactionSignedEcRecovered, revm::primitives::ResultAndState};
use sslab_execution::types::IndexedEthereumTransaction;

use crate::{
    address_based_conflict_graph::KeyBasedConflictGraph,
    nezha_core::ScheduledInfo,
    types::{Address, Key, SimulatedTransactionV2},
};

const CONTRACT_ADDR: &str = "0x1000000000000000000000000000000000000000";

#[inline]
fn _default_result_and_state() -> ResultAndState {
    ResultAndState {
        result: ExecutionResult::Success {
            reason: reth::revm::primitives::SuccessReason::Stop,
            gas_used: 0,
            gas_refunded: 0,
            logs: vec![],
            output: reth::revm::primitives::Output::Call(vec![].into()),
        },
        state: Default::default(),
    }
}

#[inline]
fn _make_set(contract_addr: &str, set: HashSet<Key>) -> HashMap<Address, HashSet<Key>> {
    let mut map = HashMap::new();
    map.insert(Address::from_str(contract_addr).unwrap(), set);
    map
}

fn transaction_with_rw(tx_id: u64, read_key: u64, write_key: u64) -> SimulatedTransactionV2 {
    let mut read_set = HashSet::new();
    read_set.insert(Key::from(read_key));

    let mut write_set = HashSet::new();
    write_set.insert(Key::from(write_key));

    SimulatedTransactionV2::new(
        _default_result_and_state(),
        _make_set(CONTRACT_ADDR, read_set),
        _make_set(CONTRACT_ADDR, write_set),
        IndexedEthereumTransaction::new(TransactionSignedEcRecovered::default(), tx_id),
    )
}

fn transaction_with_multiple_rw(
    tx_id: u64,
    read_keys: Vec<u64>,
    write_keys: Vec<u64>,
) -> SimulatedTransactionV2 {
    let read_set = read_keys
        .into_iter()
        .map(|key| Key::from(key))
        .collect::<HashSet<Key>>();

    let write_set = write_keys
        .into_iter()
        .map(|key| Key::from(key))
        .collect::<HashSet<Key>>();

    SimulatedTransactionV2::new(
        _default_result_and_state(),
        _make_set(CONTRACT_ADDR, read_set),
        _make_set(CONTRACT_ADDR, write_set),
        IndexedEthereumTransaction::new(TransactionSignedEcRecovered::default(), tx_id),
    )
}

fn transaction_with_multiple_rw_str(
    tx_id: u64,
    read_keys: Vec<&str>,
    write_keys: Vec<&str>,
) -> SimulatedTransactionV2 {
    let read_set = read_keys
        .into_iter()
        .map(|key| Key::from_str(key).unwrap())
        .collect::<HashSet<Key>>();

    let write_set = write_keys
        .into_iter()
        .map(|key| Key::from_str(key).unwrap())
        .collect::<HashSet<Key>>();

    SimulatedTransactionV2::new(
        _default_result_and_state(),
        _make_set(CONTRACT_ADDR, read_set),
        _make_set(CONTRACT_ADDR, write_set),
        IndexedEthereumTransaction::new(TransactionSignedEcRecovered::default(), tx_id),
    )
}

fn nezha_test(
    input_txs: Vec<SimulatedTransactionV2>,
    answer: (Vec<Vec<u64>>, Vec<Vec<u64>>),
    print_result: bool,
) {
    let ScheduledInfo {
        scheduled_txs,
        aborted_txs,
    } = KeyBasedConflictGraph::construct(input_txs.clone())
        .hierarchcial_sort()
        .reorder()
        .extract_schedule();

    if print_result {
        println!("Scheduled Transactions:");
        scheduled_txs.iter().for_each(|txs| {
            txs.iter().for_each(|tx| {
                print!("{} ", tx.id());
            });
            print!("\n");
        });

        println!("Aborted Transactions:");
        aborted_txs.iter().for_each(|txs| {
            txs.iter().for_each(|tx| {
                print!("{} ", tx.id());
            });
            print!("\n");
        });
    }

    let (s_ans, a_ans) = answer;

    scheduled_txs
        .iter()
        .zip(s_ans.iter())
        .for_each(|(txs, idx)| {
            assert_eq!(txs.len(), idx.len());
            let answer_set: HashSet<&u64> = idx.iter().collect();
            assert!(txs.iter().all(|tx| answer_set.contains(&tx.id())))
        });

    aborted_txs.iter().zip(a_ans.iter()).for_each(|(txs, idx)| {
        assert_eq!(txs.len(), idx.len());
        let answer_set: HashSet<&u64> = idx.iter().collect();
        assert!(txs.iter().all(|tx| answer_set.contains(&tx.id())))
    });

    aborted_txs.into_iter().flatten().for_each(|tx| {
        std::sync::Arc::try_unwrap(tx).unwrap();
    })
}

async fn nezha_par_test(
    input_txs: Vec<SimulatedTransactionV2>,
    answer: (Vec<Vec<u64>>, Vec<Vec<u64>>),
    print_result: bool,
) {
    let ScheduledInfo {
        scheduled_txs,
        aborted_txs,
    } = KeyBasedConflictGraph::par_construct(input_txs.clone())
        .await
        .hierarchcial_sort()
        .reorder()
        .par_extract_schedule()
        .await;

    if print_result {
        println!("Scheduled Transactions:");
        scheduled_txs.iter().for_each(|txs| {
            txs.iter().for_each(|tx| {
                print!("{} ", tx.id());
            });
            print!("\n");
        });
        println!("Aborted Transactions:");
        aborted_txs.iter().for_each(|txs| {
            txs.iter().for_each(|tx| {
                print!("{} ", tx.id());
            });
            print!("\n");
        });
    }

    let (s_ans, a_ans) = answer;

    scheduled_txs
        .iter()
        .zip(s_ans.iter())
        .for_each(|(txs, idx)| {
            assert_eq!(txs.len(), idx.len());
            let answer_set: HashSet<&u64> = idx.iter().collect();
            assert!(txs.iter().all(|tx| answer_set.contains(&tx.id())))
        });

    aborted_txs.iter().zip(a_ans.iter()).for_each(|(txs, idx)| {
        assert_eq!(txs.len(), idx.len());
        let answer_set: HashSet<&u64> = idx.iter().collect();
        assert!(txs.iter().all(|tx| answer_set.contains(&tx.id())))
    });

    aborted_txs.into_iter().flatten().for_each(|tx| {
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

    let first_scheduled = vec![vec![2], vec![3, 4], vec![5, 6]];

    let second_scheduled = vec![vec![1]];

    nezha_test(
        txs.clone(),
        (first_scheduled.clone(), second_scheduled.clone()),
        false,
    );
    nezha_par_test(txs.clone(), (first_scheduled, second_scheduled), false).await;
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

    let first_scheduled = vec![vec![3], vec![2], vec![4], vec![5, 6]];

    let second_scheduled = vec![vec![1]];

    nezha_test(
        txs.clone(),
        (first_scheduled.clone(), second_scheduled.clone()),
        false,
    );
    nezha_par_test(txs.clone(), (first_scheduled, second_scheduled), false).await;
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

    let first_scheduled = vec![vec![2], vec![3, 6], vec![4], vec![5]];

    let second_scheduled = vec![vec![1]];

    nezha_test(
        txs.clone(),
        (first_scheduled.clone(), second_scheduled.clone()),
        false,
    );
    nezha_par_test(txs.clone(), (first_scheduled, second_scheduled), false).await;
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

    let first_scheduled = vec![vec![1], vec![2], vec![3], vec![4]];

    let second_scheduled = vec![vec![5, 6]];

    nezha_test(
        txs.clone(),
        (first_scheduled.clone(), second_scheduled.clone()),
        false,
    );
    nezha_par_test(txs.clone(), (first_scheduled, second_scheduled), false).await;
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

    let first_scheduled = vec![vec![1], vec![2], vec![3], vec![4]];

    let second_scheduled = vec![vec![5, 6], vec![7]];

    nezha_test(
        txs.clone(),
        (first_scheduled.clone(), second_scheduled.clone()),
        false,
    );
    nezha_par_test(txs.clone(), (first_scheduled, second_scheduled), false).await;
}

#[tokio::test]
async fn test_reordering() {
    let txs = vec![
        transaction_with_multiple_rw(1, vec![], vec![1, 2]),
        transaction_with_rw(2, 2, 1),
    ];

    let first_scheduled = vec![vec![2], vec![1]];

    let second_scheduled = vec![];

    nezha_test(
        txs.clone(),
        (first_scheduled.clone(), second_scheduled.clone()),
        false,
    );
    nezha_par_test(txs.clone(), (first_scheduled, second_scheduled), false).await;
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

    let first_scheduled = vec![vec![1, 2]];

    let second_scheduled = vec![vec![3]];

    nezha_test(
        txs.clone(),
        (first_scheduled.clone(), second_scheduled.clone()),
        false,
    );
    nezha_par_test(txs.clone(), (first_scheduled, second_scheduled), false).await;
}
