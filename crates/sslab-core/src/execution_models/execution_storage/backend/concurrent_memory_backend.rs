use std::fmt::Debug;
use ethers_core::types::{U256, H256, H160};
use evm::backend::{MemoryVicinity, Backend, Basic, Apply};
use super::{ApplyBackend, ConcurrentHashMap};


#[derive(Debug, Default, Clone)]
pub struct CAccount {
    	/// Account nonce.
	pub nonce: U256,
	/// Account balance.
	pub balance: U256,
	/// Full account storage.
	pub storage: ConcurrentHashMap<H256, H256>,
	/// Account code.
	pub code: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct CMemoryBackend {
    vicinity: MemoryVicinity,
    state: ConcurrentHashMap<H160, CAccount>
}

impl CMemoryBackend {

	/// Create a new memory backend.
	pub fn new(vicinity: MemoryVicinity, state: ConcurrentHashMap<H160, CAccount>) -> Self {
		Self {
			vicinity,
			state,
		}
	}

	/// Get the underlying `BTreeMap` storing the state.
	pub fn state(&self) -> &ConcurrentHashMap<H160, CAccount> {
		&self.state
	}
}

impl Default for CMemoryBackend {
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

        CMemoryBackend::new(vicinity, ConcurrentHashMap::default())
    }
}

impl Backend for CMemoryBackend {
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
        self.state.pin().get(&address).is_some()
    }

    fn basic(&self, address: H160) -> Basic {
        self.state.pin()
            .get(&address)
            .map(|a| Basic {
                balance: a.balance,
                nonce: a.nonce,
            })
            .unwrap_or_default()
    }

    fn code(&self, address: H160) -> Vec<u8> {
        self.state.pin()
            .get(&address)
            .map(|v| v.code.clone())
            .unwrap_or_default()
    }

    fn storage(&self, address: H160, index: H256) -> H256 {
        match self.state.pin().get(&address) {
            Some(v) => {
                match v.storage.pin().get(&index) {
                    Some(v) => v.clone(),
                    None => H256::default(),
                }
            },
            None => H256::default(),
        }
    }

    fn original_storage(&self, address: H160, index: H256) -> Option<H256> {
        Some(self.storage(address, index))
    }
}

impl ApplyBackend for CMemoryBackend {
    fn apply(&self, values: Vec<Apply>, delete_empty: bool) {
        for apply in values {
			match apply {
				Apply::Modify {
					address,
					basic,
					code,
					storage,
					reset_storage,
				} => {
                    let state = self.state.pin();
					let is_empty = {
                        let mut account = state.get(&address).cloned().unwrap_or_default();

						account.balance = basic.balance;
						account.nonce = basic.nonce;
						if let Some(code) = code {
							account.code = code;
						}

						if reset_storage {
							account.storage = ConcurrentHashMap::default();
						}

						let zeros = account
							.storage.pin()
							.iter()
							.filter(|(_, value)| *value == &H256::default())
							.map(|(key, _)| key.to_owned())
							.collect::<Vec<H256>>();

						for zero in zeros.iter() {
							account.storage.pin().remove(zero);
						}

						for (index, value) in storage {
							if value == H256::default() {
								account.storage.pin().remove(&index);
							} else {
								account.storage.pin().insert(index, value);
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
					self.state.pin().remove(&address);
				}
			}
		}
	}
}