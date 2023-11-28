use std::collections::BTreeMap;
use ethers_core::types::H160;
use evm::{backend::{Log, Apply}, Config, executor::stack::PrecompileFn};

mod concurrent_memory_backend;
mod memory_backend;

pub use concurrent_memory_backend::{CMemoryBackend, CAccount};
pub use memory_backend::MemoryBackend;

pub type ConcurrentHashMap<K, V> = flurry::HashMap<K, V>;

pub struct ExecutionResult {
    pub logs: Vec<Log>,
    pub effects: Vec::<Apply>,
}

pub trait ApplyBackend {
    fn apply(&self, values: Vec<Apply>, delete_empty: bool);
}

pub trait ExecutionBackend {

    fn config(&self) -> &Config;

    fn precompiles(&self) -> &BTreeMap<H160, PrecompileFn>;

    fn code(&self, address: H160) -> Vec<u8>;

    fn apply_all_effects(&self, execution_result: &ExecutionResult);

    fn apply_local_effect(&self, effect: Vec<Apply>);
}


// #[derive(Debug, Default, Clone)]
// pub struct FlurryHashMap<K, V> 
// where
//     K: Sync + Send + Clone + Hash + Ord + PartialEq + Eq,
//     V: Sync + Send + Clone,
// {
//     pub map: flurry::HashMap<K, V>
// }


// impl <K, V> FlurryHashMap<K, V> 
// where
//     K: Sync + Send + Clone + Hash + Ord + PartialEq + Eq,
//     V: Sync + Send + Clone,
// {
//     pub fn iter(&self) -> flurry::iter::Iter<K, V> {
//         self.map.pin().iter()
//     }

//     pub fn get(&self, key: &K) -> Option<&V> {
//         self.map.pin().get(key).cloned()
//     }

//     pub fn insert(&self, key: K, value: V) {
//         self.map.pin().insert(key, value);
//     }

//     pub fn remove(&self, key: &K) {
//         self.map.pin().remove(key);
//     }
// }

// #[derive(Debug, Default, Clone)]
// pub struct DashMap<K, V> 
// where
//     K: Sync + Send + Clone + Hash + Eq + PartialEq,
//     V: Sync + Send,
// {
//     pub map: dashmap::DashMap<K, V>
// }


// impl<K, V> DashMap<K, V> 
// where
//     K: Sync + Send + Clone + Hash + Eq + PartialEq,
//     V: Sync + Send,
// {
//     pub fn iter(&self) -> dashmap::iter::Iter<K, V> {
//         self.map.iter()
//     }

//     pub fn get(&self, key: &K) -> Option<dashmap::mapref::one::Ref<K, V>> {
//         self.map.get(key) 
//     }

//     pub fn insert(&self, key: K, value: V) {
//         self.map.insert(key, value);
//     }

//     pub fn remove(&self, key: &K) {
//         self.map.remove(key);
//     }
// }

// #[derive(Debug, Default, Clone)]
// pub struct ShardedMap<K, V> 
// where
//     K: Sync + Send + Clone + Hash + Eq + PartialEq,
//     V: Sync + Send,
// {
//     pub map: sharded::Map<K, V>
// }


// impl <K, V>  ShardedMap<K, V>
// where
//     K: Sync + Send + Clone + Hash + Eq + PartialEq,
//     V: Sync + Send,
// {
//     pub fn iter(&self) -> sharded::Iter<K, V> {
//         self.map.iter()
//     }

//     pub fn get(&self, key: &K) -> Option<&V> {
//         self.map.read(key)
//     }

//     pub fn insert(&self, key: K, value: V) {
//         self.map.insert(key, value);
//     }

//     pub fn remove(&self, key: &K) {
//         self.map.remove(key.clone());
//     }
// } 


// pub struct LeapfrogMap<K, V> 
// where
//     K: Sync + Send + Clone + Hash + Eq + PartialEq,
//     V: Sync + Send,
// {
//     pub map: leapfrog::LeapMap<K, V>
// }

// impl<K, V> LeapfrogMap<K, V> 
// where
//     K: Sync + Send + Clone + Hash + Eq + PartialEq + std::marker::Copy,
//     V: Sync + Send + leapfrog::Value + std::marker::Copy + Clone + std::fmt::Debug,
// {
//     pub fn iter(&self) {
//         self.map.iter()
//     }

//     pub fn get(&self, key: &K) -> Option<&V> {
//         self.map.get(key)
//     }

//     pub fn insert(&self, key: K, value: V) {
//         self.map.insert(key, value);
//     }

//     pub fn remove(&self, key: &K) {
//         self.map.remove(key);
//     }
// }