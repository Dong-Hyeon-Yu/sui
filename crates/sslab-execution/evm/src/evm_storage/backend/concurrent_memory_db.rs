use reth::revm::{
    db::{AccountState, DatabaseCommit, DatabaseRef, EmptyDB},
    primitives::{
        db::Database, Account, AccountInfo, Address, Bytecode, HashMap, B256, KECCAK_EMPTY, U256,
    },
};

use std::sync::Arc;

// pub enum DatabaseError {
//     NotFound,
//     Other,
// }

// #[derive(Debug, Clone, Default)]
// pub struct ConcurrentStateBackend {
//     pub accounts: ChashMap<Address, DbAccount>,
//     pub bytecodes: ChashMap<B256, Bytecode>,
//     pub block_hashes: ChashMap<U256, B256>,
// }

// impl Database for ConcurrentStateBackend {
//     #[doc = r" The database error type."]
//     type Error = DatabaseError;

//     #[doc = r" Get basic account information."]
//     fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
//         todo!()
//     }

//     #[doc = r" Get account code by its hash."]
//     fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
//         todo!()
//     }

//     #[doc = r" Get storage value of address at index."]
//     fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
//         todo!()
//     }

//     #[doc = r" Get block hash by block number."]
//     fn block_hash(&mut self, number: U256) -> Result<B256, Self::Error> {
//         todo!()
//     }
// }

// impl DatabaseRef for ConcurrentStateBackend {
//     #[doc = r" The database error type."]
//     type Error = DatabaseError;

//     #[doc = r" Get basic account information."]
//     fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
//         match self.accounts.entry(address) {
//             Entry::Occupied(entry) => Ok(Some(entry.get().info.clone())),
//             Entry::Vacant(_) => Ok(None),
//         }
//     }

//     #[doc = r" Get account code by its hash."]
//     fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
//         match self.bytecodes.entry(code_hash) {
//             Entry::Occupied(entry) => Ok(entry.get().clone()),
//             Entry::Vacant(_) => Err(DatabaseError::NotFound),
//         }
//     }

//     #[doc = r" Get storage value of address at index."]
//     fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
//         match self.accounts.entry(address) {
//             Entry::Occupied(entry) => match entry.get().storage.get(&index) {
//                 Some(value) => Ok(*value),
//                 None => Ok(U256::ZERO),
//             },
//             Entry::Vacant(_) => Ok(U256::ZERO),
//         }
//     }

//     #[doc = r" Get block hash by block number."]
//     fn block_hash_ref(&self, number: U256) -> Result<B256, Self::Error> {
//         match self.block_hashes.entry(number) {
//             Entry::Occupied(entry) => Ok(*entry.get()),
//             Entry::Vacant(_) => Err(DatabaseError::NotFound),
//         }
//     }
// }

// impl DatabaseCommit for ConcurrentStateBackend {
//     #[doc = r" Commit changes to the database."]
//     fn commit(&mut self, changes: HashMap<Address, Account>) {
//         todo!()
//     }
// }

/// A [Database] implementation that stores all state changes in memory.
pub type InMemoryConcurrentDB = CacheDB<EmptyDB>;

type ChashMap<K, V> = flurry::HashMap<K, V>;

/// A [Database] implementation that stores all state changes in memory.
///
/// This implementation wraps a [DatabaseRef] that is used to load data ([AccountInfo]).
///
/// Accounts and code are stored in two separate maps, the `accounts` map maps addresses to [DbAccount],
/// whereas contracts are identified by their code hash, and are stored in the `contracts` map.
/// The [DbAccount] holds the code hash of the contract, which is used to look up the contract in the `contracts` map.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CacheDB<ExtDB> {
    /// Account info where None means it is not existing. Not existing state is needed for Pre TANGERINE forks.
    /// `code` is always `None`, and bytecode can be found in `contracts`.
    pub accounts: Arc<ChashMap<Address, DbAccount>>,
    /// Tracks all contracts by their code hash.
    pub contracts: Arc<ChashMap<B256, Bytecode>>,
    /// All logs that were committed via [DatabaseCommit::commit].
    // pub logs: Vec<Log>,
    /// All cached block hashes from the [DatabaseRef].
    pub block_hashes: Arc<ChashMap<U256, B256>>,
    /// The underlying database ([DatabaseRef]) that is used to load data.
    ///
    /// Note: this is read-only, data is never written to this database.
    pub db: ExtDB,
}

impl<ExtDB: Default + Clone> Default for CacheDB<ExtDB> {
    fn default() -> Self {
        Self::new(ExtDB::default())
    }
}

impl<ExtDB: Clone> CacheDB<ExtDB> {
    pub fn new(db: ExtDB) -> Self {
        let contracts = ChashMap::new();
        contracts.pin().insert(KECCAK_EMPTY, Bytecode::new());
        contracts.pin().insert(B256::ZERO, Bytecode::new());
        Self {
            accounts: Arc::new(ChashMap::new()),
            contracts: Arc::new(contracts),
            // logs: Vec::default(),
            block_hashes: Arc::new(ChashMap::new()),
            db,
        }
    }

    /// Inserts the account's code into the cache.
    ///
    /// Accounts objects and code are stored separately in the cache, this will take the code from the account and instead map it to the code hash.
    ///
    /// Note: This will not insert into the underlying external database.
    pub fn insert_contract(&self, account: &mut AccountInfo) {
        if let Some(code) = &account.code {
            if !code.is_empty() {
                if account.code_hash == KECCAK_EMPTY {
                    account.code_hash = code.hash_slow();
                }
                self.contracts
                    .pin()
                    .try_insert(account.code_hash, code.clone())
                    .ok();
            }
        }
        if account.code_hash == B256::ZERO {
            account.code_hash = KECCAK_EMPTY;
        }
    }

    /// Insert account info but not override storage
    pub fn insert_account_info(&self, address: Address, mut info: AccountInfo) {
        self.insert_contract(&mut info);
        let mut account = self
            .accounts
            .pin()
            .get(&address)
            .cloned()
            .unwrap_or_default();
        account.info = info;
        self.accounts.pin().insert(address, account);
    }
}

impl<ExtDB: DatabaseRef> CacheDB<ExtDB> {
    /// Returns the account for the given address.
    ///
    /// If the account was not found in the cache, it will be loaded from the underlying database.
    pub fn load_account(&self, address: Address) -> Result<DbAccount, ExtDB::Error> {
        match self.accounts.pin().get(&address) {
            Some(account) => Ok(account.clone()),
            None => {
                self.accounts.pin().insert(
                    address,
                    self.db
                        .basic_ref(address)?
                        .map(|info| DbAccount {
                            info,
                            ..Default::default()
                        })
                        .unwrap_or_else(DbAccount::new_not_existing),
                );

                Ok(self.accounts.pin().get(&address).cloned().unwrap())
            }
        }
    }

    /// insert account storage without overriding account info
    pub fn insert_account_storage(
        &self,
        address: Address,
        slot: U256,
        value: U256,
    ) -> Result<(), ExtDB::Error> {
        let account = self.load_account(address)?;
        account.storage.pin().insert(slot, value);
        self.accounts.pin().insert(address, account);
        Ok(())
    }

    /// replace account storage without overriding account info
    pub fn replace_account_storage(
        &self,
        address: Address,
        storage: HashMap<U256, U256>,
    ) -> Result<(), ExtDB::Error> {
        let mut account = self.load_account(address)?;
        account.account_state = AccountState::StorageCleared;
        account.storage = storage.into_iter().collect();
        self.accounts.pin().insert(address, account);
        Ok(())
    }
}

impl<ExtDB: Clone> DatabaseCommit for CacheDB<ExtDB> {
    fn commit(&self, changes: HashMap<Address, Account>) {
        for (address, mut account) in changes {
            if !account.is_touched() {
                continue;
            }
            if account.is_selfdestructed() {
                let mut db_account = self
                    .accounts
                    .pin()
                    .get(&address)
                    .cloned()
                    .unwrap_or_default();
                db_account.storage.pin().clear();
                db_account.account_state = AccountState::NotExisting;
                db_account.info = AccountInfo::default();
                self.accounts.pin().insert(address, db_account);
                continue;
            }
            let is_newly_created = account.is_created();
            self.insert_contract(&mut account.info);

            let mut db_account = self
                .accounts
                .pin()
                .get(&address)
                .cloned()
                .unwrap_or_default();
            db_account.info = account.info;

            db_account.account_state = if is_newly_created {
                db_account.storage.pin().clear();
                AccountState::StorageCleared
            } else if db_account.account_state.is_storage_cleared() {
                // Preserve old account state if it already exists
                AccountState::StorageCleared
            } else {
                AccountState::Touched
            };

            {
                let _pin = db_account.storage.pin();
                account.storage.iter().for_each(|(key, value)| {
                    _pin.insert(*key, value.present_value());
                });
            }
            self.accounts.pin().insert(address, db_account);
        }
    }
}

impl<ExtDB: DatabaseRef> Database for CacheDB<ExtDB> {
    type Error = ExtDB::Error;

    fn basic(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        match self.accounts.pin().get(&address) {
            Some(basic) => Ok(basic.info()),
            None => {
                let new_account = self
                    .db
                    .basic_ref(address)?
                    .map(|info| DbAccount {
                        info,
                        ..Default::default()
                    })
                    .unwrap_or_else(DbAccount::new_not_existing);

                let basic_info = new_account.info();

                self.accounts.pin().insert(address, new_account);

                Ok(basic_info)
            }
        }
    }

    fn code_by_hash(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        match self.contracts.pin().get(&code_hash) {
            Some(bytecode) => Ok(bytecode.clone()),
            None => {
                let bytecode = self.db.code_by_hash_ref(code_hash)?;
                self.contracts.pin().insert(code_hash, bytecode.clone());
                Ok(bytecode)
            }
        }
    }

    /// Get the value in an account's storage slot.
    ///
    /// It is assumed that account is already loaded.
    fn storage(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        match self.accounts.pin().get(&address) {
            Some(account) => {
                if let Some(slot) = account.storage.pin().get(&index) {
                    return Ok(*slot);
                }

                if matches!(
                    account.account_state,
                    AccountState::StorageCleared | AccountState::NotExisting
                ) {
                    return Ok(U256::ZERO);
                } else {
                    let slot = self.db.storage_ref(address, index)?;
                    account.storage.pin().insert(index, slot);
                    return Ok(slot);
                }
            }
            None => {
                let info = self.db.basic_ref(address)?;
                let value = if info.is_some() {
                    let value = self.db.storage_ref(address, index)?;
                    let account: DbAccount = info.into();
                    account.storage.pin().insert(index, value);
                    value
                } else {
                    let value = U256::ZERO;
                    let account = info.into();
                    self.accounts.pin().insert(address, account);
                    value
                };

                Ok(value)
            }
        }
    }

    fn block_hash(&self, number: U256) -> Result<B256, Self::Error> {
        match self.block_hashes.pin().get(&number) {
            Some(block_hash) => Ok(*block_hash),
            None => {
                let hash = self.db.block_hash_ref(number)?;
                self.block_hashes.pin().insert(number, hash);
                Ok(hash)
            }
        }
    }
}

impl<ExtDB: DatabaseRef> DatabaseRef for CacheDB<ExtDB> {
    type Error = ExtDB::Error;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        match self.accounts.pin().get(&address) {
            Some(acc) => Ok(acc.info()),
            None => self.db.basic_ref(address),
        }
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        match self.contracts.pin().get(&code_hash) {
            Some(entry) => Ok(entry.clone()),
            None => self.db.code_by_hash_ref(code_hash),
        }
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        match self.accounts.pin().get(&address) {
            Some(acc_entry) => match acc_entry.storage.pin().get(&index) {
                Some(entry) => Ok(*entry),
                None => {
                    if matches!(
                        acc_entry.account_state,
                        AccountState::StorageCleared | AccountState::NotExisting
                    ) {
                        Ok(U256::ZERO)
                    } else {
                        self.db.storage_ref(address, index)
                    }
                }
            },
            None => self.db.storage_ref(address, index),
        }
    }

    fn block_hash_ref(&self, number: U256) -> Result<B256, Self::Error> {
        match self.block_hashes.pin().get(&number) {
            Some(entry) => Ok(*entry),
            None => self.db.block_hash_ref(number),
        }
    }
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DbAccount {
    pub info: AccountInfo,
    /// If account is selfdestructed or newly created, storage will be cleared.
    pub account_state: AccountState,
    /// storage slots
    pub storage: ChashMap<U256, U256>,
}

impl DbAccount {
    pub fn new_not_existing() -> Self {
        Self {
            account_state: AccountState::NotExisting,
            ..Default::default()
        }
    }

    pub fn info(&self) -> Option<AccountInfo> {
        if matches!(self.account_state, AccountState::NotExisting) {
            None
        } else {
            Some(self.info.clone())
        }
    }
}

impl From<Option<AccountInfo>> for DbAccount {
    fn from(from: Option<AccountInfo>) -> Self {
        from.map(Self::from).unwrap_or_else(Self::new_not_existing)
    }
}

impl From<AccountInfo> for DbAccount {
    fn from(info: AccountInfo) -> Self {
        Self {
            info,
            account_state: AccountState::None,
            ..Default::default()
        }
    }
}

/// Custom benchmarking DB that only has account info for the zero address.
///
/// Any other address will return an empty account.
// #[derive(Debug, Default, Clone)]
// pub struct BenchmarkDB(pub Bytecode, B256);

// impl BenchmarkDB {
//     pub fn new_bytecode(bytecode: Bytecode) -> Self {
//         let hash = bytecode.hash_slow();
//         Self(bytecode, hash)
//     }
// }

// impl Database for BenchmarkDB {
//     type Error = Infallible;
//     /// Get basic account information.
//     fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
//         if address == Address::ZERO {
//             return Ok(Some(AccountInfo {
//                 nonce: 1,
//                 balance: U256::from(10000000),
//                 code: Some(self.0.clone()),
//                 code_hash: self.1,
//             }));
//         }
//         if address == Address::with_last_byte(1) {
//             return Ok(Some(AccountInfo {
//                 nonce: 0,
//                 balance: U256::from(10000000),
//                 code: None,
//                 code_hash: KECCAK_EMPTY,
//             }));
//         }
//         Ok(None)
//     }

//     /// Get account code by its hash
//     fn code_by_hash(&mut self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
//         Ok(Bytecode::default())
//     }

//     /// Get storage value of address at index.
//     fn storage(&mut self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
//         Ok(U256::default())
//     }

//     // History related
//     fn block_hash(&mut self, _number: U256) -> Result<B256, Self::Error> {
//         Ok(B256::default())
//     }
// }

#[cfg(test)]
mod tests {
    use reth::revm::db::{CacheDB, EmptyDB};
    use reth::revm::primitives::{db::Database, AccountInfo, Address, U256};

    #[test]
    fn test_insert_account_storage() {
        let account = Address::with_last_byte(42);
        let nonce = 42;
        let mut init_state = CacheDB::new(EmptyDB::default());
        init_state.insert_account_info(
            account,
            AccountInfo {
                nonce,
                ..Default::default()
            },
        );

        let (key, value) = (U256::from(123), U256::from(456));
        let mut new_state = CacheDB::new(init_state);
        let _ = new_state.insert_account_storage(account, key, value);

        assert_eq!(new_state.basic(account).unwrap().unwrap().nonce, nonce);
        assert_eq!(new_state.storage(account, key), Ok(value));
    }

    #[test]
    fn test_replace_account_storage() {
        let account = Address::with_last_byte(42);
        let nonce = 42;
        let mut init_state = CacheDB::new(EmptyDB::default());
        init_state.insert_account_info(
            account,
            AccountInfo {
                nonce,
                ..Default::default()
            },
        );

        let (key0, value0) = (U256::from(123), U256::from(456));
        let (key1, value1) = (U256::from(789), U256::from(999));
        let _ = init_state.insert_account_storage(account, key0, value0);

        let mut new_state = CacheDB::new(init_state);
        let _ = new_state.replace_account_storage(account, [(key1, value1)].into());

        assert_eq!(new_state.basic(account).unwrap().unwrap().nonce, nonce);
        assert_eq!(new_state.storage(account, key0), Ok(U256::ZERO));
        assert_eq!(new_state.storage(account, key1), Ok(value1));
    }

    #[cfg(feature = "serde-json")]
    #[test]
    fn test_serialize_deserialize_cachedb() {
        let account = Address::with_last_byte(69);
        let nonce = 420;
        let mut init_state = CacheDB::new(EmptyDB::default());
        init_state.insert_account_info(
            account,
            AccountInfo {
                nonce,
                ..Default::default()
            },
        );

        let serialized = serde_json::to_string(&init_state).unwrap();
        let deserialized: CacheDB<EmptyDB> = serde_json::from_str(&serialized).unwrap();

        assert!(deserialized.accounts.get(&account).is_some());
        assert_eq!(
            deserialized.accounts.get(&account).unwrap().info.nonce,
            nonce
        );
    }
}
