use std::{sync::Arc, collections::{BTreeMap, HashMap}};
use ethers_core::types::{H256, H160};
use evm::{backend::{Apply, Log}, executor::stack::RwSet};
use itertools::Itertools;

use parking_lot::{RwLock, RwLockReadGuard};
use rayon::prelude::*;
use sslab_execution::types::EthereumTransaction;

use super::{types::SimulatedTransaction, nezha_core::ScheduledInfo};


// pub(crate) type FastHashMap<K, V> = hashbrown::HashMap<K, V, nohash_hasher::BuildNoHashHasher<K>>;
pub(crate) type FastHashSet<K> = hashbrown::HashSet<K, nohash_hasher::BuildNoHashHasher<K>>;


pub struct AddressBasedConflictGraph {
    addresses: hashbrown::HashMap<H256, Address>,
    tx_list: hashbrown::HashMap<H256, Arc<Transaction>>, // tx_id -> transaction
    aborted_txs: Vec<Arc<Transaction>>, // optimization for reordering.
}

impl AddressBasedConflictGraph {

    fn new() -> Self {
        Self {
            addresses: hashbrown::HashMap::new(),
            tx_list: hashbrown::HashMap::new(),
            aborted_txs: Vec::new(),
        }
    }


    pub fn construct(simulation_result: Vec<SimulatedTransaction>) -> Self {

        let mut acg = Self::new();

        for tx in simulation_result {

            let (_tx, rw_set) = Transaction::from(tx);
            let tx_metadata = Arc::new(_tx);

            let (read_set, write_set) = rw_set.destruct();
            let mut write_units = Self::_convert_to_units(&tx_metadata, UnitType::Write, write_set, Some(&read_set));
            let mut read_units = Self::_convert_to_units(&tx_metadata, UnitType::Read, read_set, None);

            // before inserting the units, wr-dependencies must be created b/w RW units.
            Self::_set_wr_dependencies(&mut read_units, &mut write_units);

            tx_metadata.set_write_units(write_units.clone());

            acg.tx_list.insert(tx_metadata.id(), tx_metadata);
            acg._add_units_to_address([read_units, write_units].concat());
        }

        acg
    }

    pub async fn par_construct(simulation_result: Vec<SimulatedTransaction>) -> Self {
        let num_of_txn = simulation_result.len();
        let ncpu = num_cpus::get();

        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let mut sub_graphs = simulation_result
                .par_chunks(std::cmp::max(num_of_txn / ncpu, 1))
                .map(|chunk| {
                    Self::construct(chunk.to_vec())
                })
                .collect::<Vec<Self>>();

            while sub_graphs.len() > 1 {
                sub_graphs = sub_graphs
                    .into_par_iter()
                    .chunks(2)
                    .map(|chuck| {
                        if chuck.len() == 1 {
                            chuck.into_iter().next().unwrap()
                        } else {
                            let (mut left, right) = chuck.into_iter().next_tuple().unwrap();
                            left.merge(right);
                            left
                        }
                    })
                    .collect::<Vec<Self>>();
            }
            
            let result = sub_graphs.into_iter().next().unwrap();
            let _ = send.send(result);
        });

        recv.await.unwrap()
    }
    

    pub fn hierarchcial_sort(&mut self) -> &mut Self {  //? Radix sort?

        for addr_key in &self._address_rank() {

            let current_addr = self.addresses.get_mut(addr_key).unwrap();

            current_addr.sort_read_units();
            current_addr.sort_write_units();
        };

        self
    }


    pub fn reorder(&mut self) -> &mut Self {

        let (reorder_targets, aborted) = self._aborted_txs().into_iter().partition(|tx| tx.reorderable());

        self.aborted_txs = aborted;

        reorder_targets.iter()
            .for_each(|tx| {
                let seq = tx.write_units().iter()
                    .map(|unit| unit.address())
                    .unique()  
                    .map(|addr| {
                        let address = self.addresses.get(addr).unwrap();
                        
                        address.write_units.max_seq().max(address.read_units.max_seq())
                    })
                    .max()
                    .unwrap()
                    .clone();

                tx.set_sequence(seq);

                self.tx_list.insert(tx.id(), tx.to_owned());
            });

        self
    }


    #[must_use]
    pub fn extract_schedule(&mut self) -> ScheduledInfo {
        let tx_list = std::mem::replace(&mut self.tx_list, hashbrown::HashMap::default());
        // let aborted_txs = std::mem::replace(&mut self.aborted_txs, Vec::new());

        tx_list.iter().for_each(|(_, tx)| tx.clear_write_units());
        // aborted_txs.iter().for_each(|tx| tx.clear_write_units());
        self.addresses.clear();
        self.addresses.shrink_to_fit();

        ScheduledInfo::from(tx_list, self.aborted_txs.len())
    }

    pub async fn par_extract_schedule(&mut self) -> (ScheduledInfo, Vec<EthereumTransaction>) {
        let tx_list = std::mem::replace(&mut self.tx_list, hashbrown::HashMap::default());
        let aborted_num = self.aborted_txs.len();
        let aborted_txs = std::mem::replace(&mut self.aborted_txs, Vec::new());
        
        self.addresses.clear();
        self.addresses.shrink_to_fit();

        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            tx_list.par_iter().for_each(|(_, tx)| tx.clear_write_units());
            let aborted_txs = aborted_txs
                .par_iter()
                .map(|tx| {
                    tx.clear_write_units();
                    tx.raw_tx.clone()
                })
                .collect::<Vec<_>>();

            let schedule = ScheduledInfo::par_from(tx_list, aborted_num);
            let _ = send.send((schedule, aborted_txs));
        });
        recv.await.unwrap()
    }


    /* (Algorithm1) */
    fn _address_rank(&self) -> Vec<H256> {
        let mut addresses = self.addresses.values().collect_vec();
        addresses.sort_by(|a, b| {

                // 1st priority
                match a.in_degree().cmp(b.in_degree()) {  
                    std::cmp::Ordering::Equal => {

                        // 2nd priority
                        match b.out_degree().cmp(a.out_degree()) { 
                            std::cmp::Ordering::Equal => {

                                // 3rd priority
                                a.address().cmp(b.address())  //TODO: 여기에 해당하는 address들간 in_degree가 가장 작고, out_degree가 가장 큰 순서대로 정렬하는게 best. (overhead?)
                            },
                            other => other
                        }
                    },
                    other => other
                }
            });

        addresses.into_iter()
            .map(|address| address.address().to_owned())
            .collect_vec()
    }


    fn _add_units_to_address(&mut self, units: Vec<Arc<Unit>>) {
        
        units.into_iter()
            .for_each(|unit| {
                let raw_address = unit.address();
                let address = match self.addresses.get_mut(raw_address) {
                    Some(address) => address,
                    None => {
                        self.addresses.insert(*raw_address, Address::new(*raw_address));
                        self.addresses.get_mut(raw_address).unwrap()
                    }
                };
                
                address.add_unit(unit);
            });
    }


    fn _convert_to_units(tx: &Arc<Transaction>, unit_type: UnitType, read_or_write_set: BTreeMap<H160, HashMap<H256, H256>>, read_set: Option<&BTreeMap<H160, HashMap<H256, H256>>>) -> Vec<Arc<Unit>> {
        read_or_write_set
            .into_iter()
            .map(|(contract_addr, state_items)| {

                state_items.into_iter()
                    .map(|(key, _)| {
                        /* mitigation for the across-contract calls: hash(contract addr + key) */
                        // let mut hasher = Sha256::new();
                        // hasher.update(address.as_bytes());
                        // hasher.update(key.as_bytes());
                        // let key = H256::from_slice(hasher.finalize().as_ref())

                        let co_locate = if let Some(read_set) = read_set {
                            match read_set.get(&contract_addr) {
                                Some(states) => {
                                    states.get(&key).is_some()
                                },
                                None => false
                            }
                        } else { false };

                        Arc::new(Unit::new(Arc::clone(tx), unit_type.clone(), key, co_locate))
                    })
                    .collect_vec()
            })
            .flatten()
            .collect_vec()
    }

    fn _set_wr_dependencies(read_units: &mut Vec<Arc<Unit>>, write_units: &mut Vec<Arc<Unit>>) {
        // 동일한 tx의 read/write set 을 넘겨받기 때문에, 모든 write->read dependency 추가해야함. (단 같은 address 내에서는 제외)
        write_units.into_iter().for_each(|write_unit| {
            let address = write_unit.address();

            read_units.iter().for_each(|read_unit| {
                if read_unit.address() != address {
                    read_unit.add_dependency();
                    write_unit.add_dependency();
                }
            });
        });
    }

    fn _aborted_txs(&mut self) -> Vec<Arc<Transaction>> {
        let aborted_tx_indice = self.tx_list.iter()
            .filter(|(_, tx)| tx.aborted())
            .map(|(tx_id, _)| tx_id.to_owned())
            .collect_vec();

        aborted_tx_indice.iter()
            .for_each(|idx| {
                let tx = self.tx_list.remove(idx).unwrap();
                self.aborted_txs.push(tx);
            });

        self.aborted_txs.clone()
    }

    fn merge(&mut self, other: Self)  {
        other.addresses
            .iter()
            .for_each(|(addr, address)| {
                match self.addresses.get_mut(addr) {
                    Some(my_address) => {
                        my_address.merge(address.to_owned());
                    },
                    None => {
                        self.addresses.insert(*addr, address.to_owned());
                    }
                }
        });
        self.tx_list.extend(other.tx_list);
    }
}



#[derive(Debug)]
pub struct Transaction {
    tx_id: H256, 
    sequence: RwLock<u64>,  // 0 represents that this transaction havn't been ordered yet.
    aborted: RwLock<bool>,
    write_units: RwLock<Vec<Arc<Unit>>>,

    effects: Vec<Apply>,
    logs: Vec<Log>,
    raw_tx: EthereumTransaction
}

impl Transaction {

    fn from(tx: SimulatedTransaction) -> (Self, RwSet) {
        let (tx_id, rw_set, effects, logs, raw_tx ) = tx.deconstruct();
        let tx = Self { 
            tx_id,
            sequence: RwLock::new(0), 
            aborted: RwLock::new(false), 
            write_units: RwLock::new(Vec::new()), 
            effects, 
            logs, 
            raw_tx 
        };

        (tx, rw_set.unwrap())
    }

    pub fn id(&self) -> H256 {
        self.tx_id.clone()
    }

    pub fn sequence(&self) -> u64 {
        self.sequence.read().to_owned()
    }

    pub fn simulation_result(self) -> (Vec<Apply>, Vec<Log>) {
        (self.effects, self.logs)
    }

    fn set_write_units(&self, write_units: Vec<Arc<Unit>>) {
        let mut my_units = self.write_units.write();
        write_units.into_iter().for_each(|u| my_units.push(u));
    }

    fn clear_write_units(&self) {
        let mut my_units = self.write_units.write();
        my_units.clear();
        my_units.shrink_to_fit();
    }

    fn abort(&self) {
        *self.aborted.write() = true;
    }

    fn aborted(&self) -> bool {
        self.aborted.read().to_owned()
    }

    fn set_sequence(&self, sequence: u64) {
        *self.sequence.write() = sequence;
    }

    fn is_sorted(&self) -> bool {
        *self.sequence.read() != 0
    }

    fn reorderable(&self) -> bool {
        self._is_write_only() && (self.write_units.read().len() > 1)
    }

    fn _is_write_only(&self) -> bool {
        self.write_units.read().iter().all(|unit| unit.degree() == 0 && !unit.co_located())
    }

    fn write_units(&self) -> RwLockReadGuard<Vec<Arc<Unit>>> {
        self.write_units.read()
    }
}

#[derive(Clone, Debug, Copy)]
enum UnitType {
    Read,
    Write,
}

#[derive(Debug)]
struct Unit {
    tx: Arc<Transaction>,
    unit_type: UnitType,
    address: H256,
    wr_dependencies: RwLock<u32>, // Vec<Arc<Unit>> is not necessary, but the degree is only used for sorting.  
    co_located: bool  // True if the read unit and write unit are in the same address.
}

impl Unit {
    fn new(tx: Arc<Transaction>, unit_type: UnitType, address: H256, co_located: bool) -> Self {
        Self { tx, unit_type, address, wr_dependencies: RwLock::new(0), co_located }
    }

    fn unit_type(&self) -> &UnitType {
        &self.unit_type
    }

    fn address(&self) -> &H256 {
        &self.address
    }

    fn degree(&self) -> u32 {
        *self.wr_dependencies.read()
    }

    fn add_dependency(&self) {
        let mut degree = self.wr_dependencies.write();
        *degree += 1;
    }

    fn is_sorted(&self) -> bool {
        self.tx.is_sorted()
    }

    fn sequence(&self) -> u64 {
        self.tx.sequence()
    }

    fn set_sequence(&self, sequence: u64) {
        self.tx.set_sequence(sequence);
    }

    fn abort_tx(&self) {
        self.tx.abort();
    }

    fn co_located(&self) -> bool {
        self.co_located
    }
}

#[derive(Clone, Debug)]
struct ReadUnits {
    units: Vec<Arc<Unit>>,
    max_seq: u64,
}

impl ReadUnits {
    fn new() -> Self {
        Self {
            units: Vec::new(),
            max_seq: 0,
        }
    }

    fn push(&mut self, unit: Arc<Unit>) {
        self.units.push(unit);
    }

    /* (Algorithm2) line 3 ~ 15 */
    fn sort(&mut self) {

        /* (Algorithm2) line 3*/
        let units = std::mem::replace(&mut self.units, Vec::new());
        let (sorted, remaining): (Vec<Arc<Unit>>, Vec<Arc<Unit>>) = units.into_iter().partition(|unit| unit.is_sorted());

        let min_seq;

        /* (Algorithm2) line 4 ~ 8 */
        if sorted.is_empty() {
            self.max_seq = 1;
            min_seq = 1;
        } 
        /* (Algorithm2) line 9 ~ 15 */
        else {
            let (_min_seq, _max_seq) = sorted.iter().map(|unit| unit.sequence()).minmax().into_option().unwrap();
            min_seq = _min_seq;
            self.max_seq = _max_seq;
        }

        /* (Algorithm2) line 4~8, 12~14 */
        remaining.iter()
            .for_each(|unit| unit.set_sequence(min_seq));

        self.units = [sorted, remaining].concat();
    }

    fn increment_and_get_max_seq(&mut self) -> u64 {
        self.max_seq += 1;
        self.max_seq.clone()
    }

    fn max_seq(&self) -> &u64 {
        &self.max_seq
    }
}

#[derive(Clone, Debug)]
struct WriteUnits {
    units: Vec<Arc<Unit>>,
    max_seq: u64
}

impl WriteUnits {
    fn new() -> Self {
        Self {
            units: Vec::new(),
            max_seq: 0,
        }
    }

    fn push(&mut self, unit: Arc<Unit>) {
        self.units.push(unit);
    }

    /* (Algorithm2) line 16 ~ 35 */
    fn sort(&mut self, read_units: &mut ReadUnits) {

        /* (Algorithm2) line 16 */
        let units = std::mem::replace(&mut self.units, Vec::new());
        let (sorted, mut remaining): (Vec<Arc<Unit>>, Vec<Arc<Unit>>) = units.into_iter().partition(|unit| unit.is_sorted());

        /* (Algorithm2) line 17 ~ 19 */
        sorted.iter().for_each(|unit|{
                if unit.co_located() {
                    unit.set_sequence(read_units.increment_and_get_max_seq());
                }
            });

        /* (Algorithm2) line 20 ~ 24 */
        sorted.iter().for_each(|unit|{
                if unit.sequence() < *read_units.max_seq() {
                    unit.abort_tx();  
                }
            });

        /* (Algorithm2) line 25 ~ 29 */
        let mut write_seq = read_units.increment_and_get_max_seq();

        /* (Algorithm2) line 30 ~ 35 */
        let mut write_seq_set: FastHashSet<u64> = sorted.iter().map(|unit| unit.sequence().clone()).collect();
        remaining.iter_mut()
            .for_each(|unit| {
                while write_seq_set.contains(&write_seq) {
                    write_seq += 1;
                }
                unit.set_sequence(write_seq.clone());
                write_seq_set.insert(write_seq.clone());
            });

        self.max_seq = write_seq;  // for reordering.

        self.units = [sorted, remaining].concat();
    }

    fn max_seq(&self) -> &u64 {
        &self.max_seq
    }
}



#[derive(Clone, Debug)]
struct Address {
    address: H256,
    in_degree: u32,
    out_degree: u32,
    read_units: ReadUnits,
    write_units: WriteUnits,
}

impl Address {
    fn new(address: H256) -> Self {
        Self {
            address,
            in_degree: 0,
            out_degree: 0,
            read_units: ReadUnits::new(),
            write_units: WriteUnits::new(),
        }
    }

    fn in_degree(&self) -> &u32 {
        &self.in_degree
    }

    fn out_degree(&self) -> &u32 {
        &self.out_degree
    }

    fn address(&self) -> &H256 {
        &self.address
    }

    fn add_unit(&mut self, unit: Arc<Unit>) {

        match unit.unit_type() {
            UnitType::Read => { 
                self.in_degree += unit.degree();
                self.read_units.push(unit);
            },
            UnitType::Write => {
                self.out_degree += unit.degree();
                self.write_units.push(unit);
            }
        }
    }

    fn sort_read_units(&mut self) {
        self.read_units.sort();
    }

    fn sort_write_units(&mut self) {
        self.write_units.sort(&mut self.read_units);
    }

    fn merge(&mut self, other: Self) {
        self.in_degree += other.in_degree;
        self.out_degree += other.out_degree;
        self.read_units.units.extend(other.read_units.units);
        self.write_units.units.extend(other.write_units.units);
    }
}