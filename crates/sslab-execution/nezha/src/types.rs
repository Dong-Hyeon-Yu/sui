use crate::address_based_conflict_graph::{KdgKey, Transaction};
use itertools::Itertools;
use narwhal_types::BatchDigest;
use reth::{
    primitives::{Log, TransactionSigned, TxType, B256},
    revm::{
        db::BundleState,
        primitives::{ExecutionResult, HashMap, HashSet, ResultAndState, State},
    },
};

pub type Address = reth::revm::primitives::Address;
pub type Key = reth::revm::primitives::U256;
pub type RwSet = (
    HashMap<Address, HashSet<Key>>,
    HashMap<Address, HashSet<Key>>,
);

pub(crate) trait TransactionSignedEmbeding {
    fn restore_signed_tx(&mut self) -> (TransactionSigned, Address);
    fn gas_used(&self) -> u64;
    fn is_success(&self) -> bool;
    fn tx_type(&self) -> TxType;
    fn logs(&self) -> Vec<Log>;
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexedEthereumTransaction {
    pub tx: TransactionSigned,
    pub signer: Address,
    pub id: u64,
}

impl IndexedEthereumTransaction {
    pub fn new(tx: TransactionSigned, id: u64) -> Self {
        let signer = tx.recover_signer().unwrap();
        Self { tx, signer, id }
    }

    pub fn data(&self) -> &TransactionSigned {
        &self.tx
    }

    pub fn digest(&self) -> B256 {
        self.tx.hash()
    }

    pub fn digest_u64(&self) -> u64 {
        self.id
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn signer(&self) -> Address {
        self.signer
    }
}

// SimulcationResult includes the batch digests and rw sets of each transctions in a ConsensusOutput.
#[derive(Clone, Debug, Default)]
pub struct SimulationResultV2 {
    pub digests: Vec<BatchDigest>,
    pub rw_sets: Vec<SimulatedTransactionV2>,
}

#[derive(Clone, Debug)]
pub struct SimulatedTransactionV2 {
    // tx_id is the unique identifier of the transaction.
    // this is used to sort the transactions deterministically while scheduling.
    pub tx_id: u64,

    // read and write set are used to generate the address based conflict graph.
    pub rw_set: Option<RwSet>,

    pub result: ExecutionResult,

    // effects are the state changes after the transaction is executed.
    // if this transaction is serializable, the effects will be applied to the global state (main memory).
    pub effects: State,

    // bundle state is used to persist the state changes to the disk.
    pub bundle: BundleState,

    pub raw_tx: IndexedEthereumTransaction,
}

impl SimulatedTransactionV2 {
    /// when successfully get the [ResultAndState] of the transaction, creates a new SimulatedTransaction.
    pub fn new(
        result: ResultAndState,
        rw_set: RwSet,
        raw_tx: IndexedEthereumTransaction,
        bundle: BundleState,
    ) -> Self {
        let ResultAndState { result, state } = result;

        Self {
            tx_id: raw_tx.id,
            result,
            effects: state,
            rw_set: Some(rw_set),
            raw_tx,
            bundle,
        }
    }

    pub fn write_set(&self) -> Option<HashSet<KdgKey>> {
        match self.rw_set {
            Some((_, ref write_set)) => Some(
                write_set
                    .iter()
                    .flat_map(|(addr, keys)| {
                        keys.iter()
                            .map(|k| KdgKey {
                                address: *addr,
                                state_key: *k,
                            })
                            .collect_vec()
                    })
                    .collect::<HashSet<KdgKey>>(),
            ),
            None => None,
        }
    }
}

impl TransactionSignedEmbeding for SimulatedTransactionV2 {
    fn restore_signed_tx(&mut self) -> (TransactionSigned, Address) {
        (
            std::mem::take(&mut self.raw_tx.tx),
            std::mem::take(&mut self.raw_tx.signer),
        )
    }

    fn gas_used(&self) -> u64 {
        self.result.gas_used()
    }

    fn is_success(&self) -> bool {
        self.result.is_success()
    }

    fn tx_type(&self) -> TxType {
        self.raw_tx.tx.tx_type()
    }

    fn logs(&self) -> Vec<Log> {
        self.result.logs().into_iter().map(Into::into).collect()
    }
}

#[derive(Debug)]
pub struct ScheduledTransaction {
    pub seq: u64,
    pub tx_id: u64,

    pub result: ExecutionResult,

    // effects are the state changes after the transaction is executed.
    // if this transaction is serializable, the effects will be applied to the global state (main memory).
    pub effects: State,

    // bundle state is used to persist the state changes to the disk.
    pub bundle: BundleState,
    pub tx_type: TxType,

    pub raw_tx: Option<IndexedEthereumTransaction>,
}

impl ScheduledTransaction {
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn extract(self) -> State {
        self.effects
    }

    #[allow(dead_code)] // this function is used in unit tests.
    pub(crate) fn id(&self) -> u64 {
        self.tx_id
    }
}

impl TransactionSignedEmbeding for ScheduledTransaction {
    fn restore_signed_tx(&mut self) -> (TransactionSigned, Address) {
        let IndexedEthereumTransaction { tx, signer, .. } = self.raw_tx.take().unwrap();
        (tx, signer)
    }

    fn gas_used(&self) -> u64 {
        self.result.gas_used()
    }

    fn is_success(&self) -> bool {
        self.result.is_success()
    }

    fn tx_type(&self) -> TxType {
        self.raw_tx.as_ref().unwrap().tx.tx_type()
    }

    fn logs(&self) -> Vec<Log> {
        self.result.logs().into_iter().map(Into::into).collect()
    }
}

impl From<SimulatedTransactionV2> for ScheduledTransaction {
    fn from(tx: SimulatedTransactionV2) -> Self {
        let SimulatedTransactionV2 {
            tx_id,
            result,
            effects,
            bundle,
            raw_tx,
            ..
        } = tx;

        Self {
            seq: 0,
            tx_id,
            effects,
            result,
            bundle,
            tx_type: raw_tx.data().tx_type(),
            raw_tx: Some(raw_tx),
        }
    }
}

// impl From<std::sync::Arc<Transaction>> for ScheduledTransaction {
//     fn from(tx: std::sync::Arc<Transaction>) -> Self {
//         let tx_id = tx.id();
//         let seq = tx.sequence().to_owned();

//         Self {
//             seq,
//             tx_id,
//             effects: tx.simulation_result(),
//             result:,
//             bundle
//         }
//     }
// }

impl From<Transaction> for ScheduledTransaction {
    fn from(tx: Transaction) -> Self {
        let seq = tx.sequence();
        let Transaction {
            tx_id,
            raw_tx,
            effects,
            _execution_result,
            _bundle,
            ..
        } = tx;

        Self {
            seq,
            tx_id,
            effects,
            result: _execution_result,
            bundle: _bundle,
            tx_type: raw_tx.data().tx_type(),
            raw_tx: Some(raw_tx),
        }
    }
}
