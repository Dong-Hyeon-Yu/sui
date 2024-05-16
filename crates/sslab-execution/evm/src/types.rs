use dashmap::{
    iter::Iter,
    mapref::one::{Ref, RefMut},
};
use fastcrypto::hash::Hash;
use narwhal_types::{BatchDigest, ConsensusOutput, ConsensusOutputDigest};
use reth::primitives::{TransactionSigned, U256};

pub(crate) const NOT_SUPPORT: U256 = U256::ZERO;

#[derive(Clone, Debug, Default)]

pub struct CHashMap<K: std::hash::Hash + Eq, V>(dashmap::DashMap<K, V>);

impl<K, V> CHashMap<K, V>
where
    K: std::hash::Hash + Eq + Clone + Copy,
{
    pub fn new() -> Self {
        Self(dashmap::DashMap::new())
    }

    #[inline]
    pub fn extend<I>(&mut self, other: I)
    where
        I: IntoIterator<Item = (K, V)>,
    {
        self.0.extend(other);
    }

    #[inline]
    pub fn get(&self, key: &K) -> Option<Ref<K, V>> {
        self.0.get(key)
    }

    #[inline]
    pub fn get_mut(&self, key: &K) -> Option<RefMut<K, V>> {
        self.0.get_mut(key)
    }

    #[inline]
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.0.insert(key, value)
    }

    #[inline]
    pub fn entry(&self, key: K) -> dashmap::mapref::entry::Entry<'_, K, V> {
        self.0.entry(key)
    }

    #[inline]
    pub fn clear(&self) {
        self.0.clear();
    }

    pub fn iter(&self) -> Iter<K, V> {
        self.0.iter()
    }
}

impl<K, V> FromIterator<(K, V)> for CHashMap<K, V>
where
    K: std::hash::Hash + Eq + Clone,
{
    #[inline]
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self(dashmap::DashMap::from_iter(iter))
    }
}

impl<K, V> PartialEq for CHashMap<K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: Eq,
{
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }

        self.0.iter().all(|item| {
            other
                .0
                .get(item.key())
                .map_or(false, |v| *item.value() == *v)
        })
    }
}

impl<K, V> Eq for CHashMap<K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: Eq,
{
}

impl<K, V> IntoIterator for CHashMap<K, V>
where
    K: std::hash::Hash + Eq + Clone,
{
    type Item = (K, V);
    type IntoIter = dashmap::iter::OwningIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<K, V> From<reth::revm::primitives::hash_map::HashMap<K, V>> for CHashMap<K, V>
where
    K: std::hash::Hash + Eq + Clone + Copy,
{
    fn from(map: reth::revm::primitives::hash_map::HashMap<K, V>) -> Self {
        let mut new_map = CHashMap::new();
        new_map.extend(map.into_iter());
        new_map
    }
}
#[derive(Clone, Debug, Default)]
pub struct ExecutableEthereumBatch {
    pub digest: BatchDigest,
    pub data: Vec<TransactionSigned>,
}

impl ExecutableEthereumBatch {
    pub fn new(batch: Vec<TransactionSigned>, digest: BatchDigest) -> Self {
        Self {
            data: batch,
            digest,
        }
    }

    pub fn digest(&self) -> &BatchDigest {
        &self.digest
    }

    pub fn data(&self) -> &Vec<TransactionSigned> {
        &self.data
    }

    pub fn take_data(self) -> Vec<TransactionSigned> {
        self.data
    }
}

#[derive(Clone, Debug)]
pub struct ExecutableConsensusOutput {
    digest: ConsensusOutputDigest,
    data: Vec<ExecutableEthereumBatch>,
    timestamp: u64,
    round: u64,
    sub_dag_index: u64,
}

impl ExecutableConsensusOutput {
    pub fn new(data: Vec<ExecutableEthereumBatch>, consensus_output: &ConsensusOutput) -> Self {
        Self {
            digest: consensus_output.digest(),
            data,
            timestamp: consensus_output.sub_dag.commit_timestamp(),
            round: consensus_output.sub_dag.leader_round(),
            sub_dag_index: consensus_output.sub_dag.sub_dag_index,
        }
    }

    pub fn digest(&self) -> &ConsensusOutputDigest {
        &self.digest
    }

    pub fn take_data(self) -> Vec<ExecutableEthereumBatch> {
        self.data
    }

    pub fn data(&self) -> &Vec<ExecutableEthereumBatch> {
        &self.data
    }

    pub fn timestamp(&self) -> &u64 {
        &self.timestamp
    }

    pub fn round(&self) -> &u64 {
        &self.round
    }

    pub fn sub_dag_index(&self) -> &u64 {
        &self.sub_dag_index
    }
}

pub struct ExecutionResult {
    pub digests: Vec<BatchDigest>,
}

impl ExecutionResult {
    pub fn new(digests: Vec<BatchDigest>) -> Self {
        Self { digests }
    }

    pub fn iter(&self) -> impl Iterator<Item = &BatchDigest> {
        self.digests.iter()
    }
}
