use std::{collections::BTreeMap, sync::Arc};

use ethers_core::types::{H160, U256, H256};
use evm::backend::{MemoryVicinity, MemoryAccount, Backend, Basic, Apply};
use parking_lot::RwLock;


use super::ApplyBackend;


#[derive(Clone, Debug)]
pub struct MemoryBackend {
    vicinity: MemoryVicinity,
    state: Arc<RwLock<BTreeMap<H160, MemoryAccount>>>,
}

impl MemoryBackend {
	/// Create a new memory backend.
	pub fn new(vicinity: MemoryVicinity, state: BTreeMap<H160, MemoryAccount>) -> Self {
		Self {
			vicinity,
			state: Arc::new(RwLock::new(state)),
		}
	}
}


impl Default for MemoryBackend {
    fn default() -> Self {
        let vicinity = MemoryVicinity {
            gas_price: U256::zero(),
            origin: H160::default(),
            chain_id: U256::zero(),
            block_hashes: Vec::new(),
            block_number: Default::default(),
            block_coinbase: Default::default(),
            block_timestamp: Default::default(),
            block_difficulty: Default::default(),
            block_gas_limit: Default::default(),
            block_base_fee_per_gas: U256::zero(), //Gwei
            block_randomness: None,
        };

        MemoryBackend::new(vicinity, BTreeMap::new())
    }
}

impl Backend for MemoryBackend {
    fn gas_price(&self) -> U256 {
        self.vicinity.gas_price
    }
    fn origin(&self) -> H160 {
        self.vicinity.origin
    }
    fn block_hash(&self, number: U256) -> H256 {
        if number >= self.vicinity.block_number
            || self.vicinity.block_number - number - U256::one()
                >= U256::from(self.vicinity.block_hashes.len())
        {
            H256::default()
        } else {
            let index = (self.vicinity.block_number - number - U256::one()).as_usize();
            self.vicinity.block_hashes[index]
        }
    }
    fn block_number(&self) -> U256 {
        self.vicinity.block_number
    }
    fn block_coinbase(&self) -> H160 {
        self.vicinity.block_coinbase
    }
    fn block_timestamp(&self) -> U256 {
        self.vicinity.block_timestamp
    }
    fn block_difficulty(&self) -> U256 {
        self.vicinity.block_difficulty
    }
    fn block_randomness(&self) -> Option<H256> {
        self.vicinity.block_randomness
    }
    fn block_gas_limit(&self) -> U256 {
        self.vicinity.block_gas_limit
    }
    fn block_base_fee_per_gas(&self) -> U256 {
        self.vicinity.block_base_fee_per_gas
    }

    fn chain_id(&self) -> U256 {
        self.vicinity.chain_id
    }

    fn exists(&self, address: H160) -> bool {
        let state = self.state.read();
        state.contains_key(&address)
    }

    fn basic(&self, address: H160) -> Basic {
        let state = self.state.read();
        state
            .get(&address)
            .map(|a| Basic {
                balance: a.balance,
                nonce: a.nonce,
            })
            .unwrap_or_default()
    }

    fn code(&self, address: H160) -> Vec<u8> {
        let state = self.state.read();
        state
            .get(&address)
            .map(|v| v.code.clone())
            .unwrap_or_default()
    }

    fn storage(&self, address: H160, index: H256) -> H256 {
        let state = self.state.read();
        state
            .get(&address)
            .map(|v| v.storage.get(&index).cloned().unwrap_or_default())
            .unwrap_or_default()
    }

    fn original_storage(&self, address: H160, index: H256) -> Option<H256> {
        Some(self.storage(address, index))
    }
}

impl ApplyBackend for MemoryBackend {
    fn apply(&self, values: Vec<Apply>, delete_empty: bool) {
        let mut state = self.state.write();
        for apply in values {
			match apply {
				Apply::Modify {
					address,
					basic,
					code,
					storage,
					reset_storage,
				} => {
					let is_empty = {
						let mut account = state.entry(address).or_insert_with(Default::default);
						account.balance = basic.balance;
						account.nonce = basic.nonce;
						if let Some(code) = code {
							account.code = code;
						}

						if reset_storage {
							account.storage = BTreeMap::new();
						}

						let zeros = account
							.storage
							.iter()
							.filter(|(_, v)| *v == &H256::default())
							.map(|(k, _)| *k)
							.collect::<Vec<H256>>();

						for zero in zeros.iter() {
							account.storage.remove(zero);
						}

						for (index, value) in storage {
							if value == H256::default() {
								account.storage.remove(&index);
							} else {
								account.storage.insert(index, value);
							}
						}

						account.balance == U256::zero()
							&& account.nonce == U256::zero()
							&& account.code.is_empty()
					};

					if is_empty && delete_empty {
						state.remove(&address);
					}
				}
				Apply::Delete { address } => {
					state.remove(&address);
				}
			}
		}
	}
}