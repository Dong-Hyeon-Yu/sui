use std::{sync::Arc, str::FromStr};
use bytes::Bytes;
use ethers_core::{types::{Signature, U256, transaction::eip2718::TypedTransaction, H160}, rand};
use ethers_providers::{Provider, MockProvider};
use ethers_signers::{LocalWallet, Signer};
use narwhal_types::BatchDigest;
use rand::Rng as _;
use rand_distr::{Zipf, Uniform, Distribution};
use rayon::prelude::*;
use crate::types::{EthereumTransaction, ExecutableEthereumBatch};
use self::small_bank::SmallBank;


pub const ADMIN_SECRET_KEY: &[u8] = &[95 as u8, 126, 251, 131, 73, 90, 235, 201, 21, 22, 203, 137, 149, 240, 205, 60, 221, 27, 81, 53, 2, 200, 90, 185, 25, 240, 166, 21, 177, 41, 49, 254];
// pub const ADMIN_ADDRESS: &str = "0xe14de1592b52481b94b99df4e9653654e14fffb6";
pub const DEFAULT_CONTRACT_ADDRESS: &str = "0x1000000000000000000000000000000000000000";
pub const DEFAULT_CHAIN_ID: u64 = 9;  // ISTANBUL 


pub struct SmallBankTransactionHandler {
    admin_wallet: LocalWallet,
    chain_id: u64,
    contract: Option<SmallBank<Provider<MockProvider>>>,
    random_op_gen: Uniform<u8>,
    val_gen: Uniform<u32>,
}

#[allow(dead_code)]
impl SmallBankTransactionHandler {
    pub fn new(provider: Provider<MockProvider>, chain_id: u64) -> SmallBankTransactionHandler {
        SmallBankTransactionHandler {
            admin_wallet: LocalWallet::from_bytes(ADMIN_SECRET_KEY.try_into().unwrap()).unwrap().with_chain_id(chain_id),
            chain_id,
            contract: Some(SmallBank::new(H160::from_str(DEFAULT_CONTRACT_ADDRESS).unwrap(), Arc::new(provider))),
            random_op_gen: Uniform::new(1, 7),
            val_gen: Uniform::new(1, 1000),
        }
    }

    pub fn create_batches(&self, batch_size: usize, batch_num: usize, zipfian_coef: f32, account_num: u64) -> Vec<ExecutableEthereumBatch> {
        let target_tnx_num = batch_size * batch_num;

        let mut buffer = (0..target_tnx_num)
            .into_par_iter()
            .map(|_| self.random_operation(zipfian_coef, account_num))
            .collect::<hashbrown::HashSet<_>>();

        while buffer.len() == target_tnx_num {
            buffer.insert(self.random_operation(zipfian_coef, account_num));
        }

        buffer.into_par_iter().collect::<Vec<_>>()
            .par_chunks_exact(batch_size)
            .map(|chunk| ExecutableEthereumBatch::new(chunk.to_vec(), BatchDigest::default()))
            .collect()
    }

    pub fn create_raw_batches(&self, batch_size: usize, batch_num: usize, zipfian_coef: f32, account_num: u64) -> Vec<Vec<bytes::Bytes>> {
        let target_tnx_num = batch_size * batch_num;

        let mut buffer = (0..target_tnx_num)
            .into_par_iter()
            .map(|_| self.random_operation_raw(zipfian_coef, account_num))
            .collect::<hashbrown::HashSet<_>>();

        while buffer.len() == target_tnx_num {
            buffer.insert(self.random_operation_raw(zipfian_coef, account_num));
        }

        buffer.into_par_iter().collect::<Vec<_>>()
            .par_chunks_exact(batch_size)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>()
    }

    pub fn random_operation(&self, zipfian_coef: f32, account_num: u64) -> EthereumTransaction {

        let acc_gen = Zipf::new(account_num, zipfian_coef).unwrap();
        let acc1 = rand::thread_rng().sample(acc_gen).to_string();
        let acc2 = rand::thread_rng().sample(acc_gen).to_string();

        let rng = &mut rand::thread_rng();
        let op = self.random_op_gen.sample(rng);
        let mut tx = match op {
            0 => self.create_account(acc1, U256::from(1_000_000), U256::from(1_000_000)),
            1 => self.amalgamate(acc1, acc2),
            2 => self.get_balance(acc1),
            3 => self.send_payment(acc1, acc2, self.random_value(rng)),
            4 => self.update_balance(acc1, self.random_value(rng)),
            5 => self.update_saving(acc1, self.random_value(rng)),
            6 => self.write_check(acc1, self.random_value(rng)),
            _ => panic!("invalid operation"),
        };

        self.get_signed(&mut tx)
    }

    pub fn random_operation_raw(&self, zipfian_coef: f32, account_num: u64) -> bytes::Bytes {

        let acc_gen = Zipf::new(account_num, zipfian_coef).unwrap();
        let acc1 = rand::thread_rng().sample(acc_gen).to_string();
        let acc2 = rand::thread_rng().sample(acc_gen).to_string();

        let rng = &mut rand::thread_rng();
        let op = self.random_op_gen.sample(rng);
        let tx = match op {
            0 => self.create_account(acc1, U256::from(1_000_000), U256::from(1_000_000)),
            1 => self.amalgamate(acc1, acc2),
            2 => self.get_balance(acc1),
            3 => self.send_payment(acc1, acc2, self.random_value(rng)),
            4 => self.update_balance(acc1, self.random_value(rng)),
            5 => self.update_saving(acc1, self.random_value(rng)),
            6 => self.write_check(acc1, self.random_value(rng)),
            _ => panic!("invalid operation"),
        };

        self.get_raw_signed(tx)
    }

    #[inline]
    fn random_value(&self, rng: &mut rand::rngs::ThreadRng) -> U256 {
        U256::from(self.val_gen.sample(rng))
    }

    fn create_account(&self, acc: String, init_check: U256, init_save: U256) -> TypedTransaction {
        let mut tx = self.contract.as_ref().unwrap().create_account(acc, init_check, init_save).tx;
        self.set_default_params(&mut tx);
        tx
    }

    fn amalgamate(&self, from: String, to: String) -> TypedTransaction {
        let mut tx = self.contract.as_ref().unwrap().amalgamate(from, to).tx;
        self.set_default_params(&mut tx);
        tx
    }

    fn get_balance(&self, addr: String) -> TypedTransaction {
        let mut tx = self.contract.as_ref().unwrap().get_balance(addr).tx;
        self.set_default_params(&mut tx);
        tx
    }

    fn send_payment(&self, from: String, to: String, amount: U256) -> TypedTransaction {
        let mut tx = self.contract.as_ref().unwrap().send_payment(from, to, amount).tx;
        self.set_default_params(&mut tx);
        tx
    }

    fn update_balance(&self, addr: String, amount: U256) -> TypedTransaction {
        let mut tx = self.contract.as_ref().unwrap().deposit_checking(addr, amount).tx;
        self.set_default_params(&mut tx);
        tx
    }

    fn update_saving(&self, addr: String, amount: U256) -> TypedTransaction {
        let mut tx = self.contract.as_ref().unwrap().update_saving(addr, amount).tx;
        self.set_default_params(&mut tx);
        tx
    }

    fn write_check(&self, addr: String, amount: U256) -> TypedTransaction  {
        let mut tx = self.contract.as_ref().unwrap().write_check(addr, amount).tx;
        self.set_default_params(&mut tx);
        tx
    }

    fn set_default_params(&self, tx: &mut TypedTransaction) {
        tx.set_from(self.admin_wallet.address())
        .set_to(H160::from_str(DEFAULT_CONTRACT_ADDRESS).unwrap())
        .set_chain_id(self.chain_id)
        .set_nonce(U256::from(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()))
        .set_gas(u64::MAX)
        .set_gas_price(U256::zero());
    }

    fn get_signed(&self, tx: &mut TypedTransaction) -> EthereumTransaction {
        let signature: Signature = self.admin_wallet.sign_transaction_sync(tx).expect("signature failed");
        let tx_bytes = tx.rlp_signed(&signature).0.to_vec();
        let rlp_signed = tx_bytes.as_slice();
        EthereumTransaction::from_rlp(rlp_signed).unwrap()
    }

    fn get_raw_signed(&self, tx: TypedTransaction) -> Bytes {
        let signature: Signature = self.admin_wallet.sign_transaction_sync(&tx).expect("signature failed");
        tx.rlp_signed(&signature).0
    }
}

/// This module was auto-generated with ethers-rs Abigen.
/// More information at: <https://github.com/gakonst/ethers-rs>
#[allow(
    clippy::enum_variant_names,
    clippy::too_many_arguments,
    clippy::upper_case_acronyms,
    clippy::type_complexity,
    dead_code,
    non_camel_case_types,
)]
pub mod small_bank {
    #[rustfmt::skip]
    const __ABI: &str = "[{\"constant\":false,\"inputs\":[{\"name\":\"acc\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"updateSaving\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"acc\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"writeCheck\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"dest\",\"type\":\"string\"},{\"name\":\"src\",\"type\":\"string\"}],\"name\":\"amalgamate\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"acc\",\"type\":\"string\"},{\"name\":\"initCheck\",\"type\":\"uint256\"},{\"name\":\"initSave\",\"type\":\"uint256\"}],\"name\":\"createAccount\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"acc\",\"type\":\"string\"}],\"name\":\"getBalance\",\"outputs\":[{\"name\":\"balance\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"dest\",\"type\":\"string\"},{\"name\":\"src\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"sendPayment\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"acc\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"depositChecking\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]";
    ///The parsed JSON ABI of the contract.
    pub static SMALLBANK_ABI: ::ethers::contract::Lazy<::ethers::core::abi::Abi> = ::ethers::contract::Lazy::new(||
    ::ethers::core::utils::__serde_json::from_str(__ABI).expect("ABI is always valid"));
    pub struct SmallBank<M>(::ethers::contract::Contract<M>);
    impl<M> ::core::clone::Clone for SmallBank<M> {
        fn clone(&self) -> Self {
            Self(::core::clone::Clone::clone(&self.0))
        }
    }
    impl<M> ::core::ops::Deref for SmallBank<M> {
        type Target = ::ethers::contract::Contract<M>;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
    impl<M> ::core::ops::DerefMut for SmallBank<M> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }
    impl<M> ::core::fmt::Debug for SmallBank<M> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
            f.debug_tuple(stringify!(SmallBank)).field(&self.address()).finish()
        }
    }
    impl<M: ::ethers::providers::Middleware> SmallBank<M> {
        /// Creates a new contract instance with the specified `ethers` client at
        /// `address`. The contract derefs to a `ethers::Contract` object.
        pub fn new<T: Into<::ethers::core::types::Address>>(
            address: T,
            client: ::std::sync::Arc<M>,
        ) -> Self {
            Self(
                ::ethers::contract::Contract::new(
                    address.into(),
                    SMALLBANK_ABI.clone(),
                    client,
                ),
            )
        }
        ///Calls the contract's `amalgamate` (0x26328cdd) function
        pub fn amalgamate(
            &self,
            dest: ::std::string::String,
            src: ::std::string::String,
        ) -> ::ethers::contract::builders::ContractCall<M, ()> {
            self.0
                .method_hash([38, 50, 140, 221], (dest, src))
                .expect("method not found (this should never happen)")
        }
        ///Calls the contract's `createAccount` (0x39d8d684) function
        pub fn create_account(
            &self,
            acc: ::std::string::String,
            init_check: ::ethers::core::types::U256,
            init_save: ::ethers::core::types::U256,
        ) -> ::ethers::contract::builders::ContractCall<M, ()> {
            self.0
                .method_hash([57, 216, 214, 132], (acc, init_check, init_save))
                .expect("method not found (this should never happen)")
        }
        ///Calls the contract's `depositChecking` (0xf2f89e89) function
        pub fn deposit_checking(
            &self,
            acc: ::std::string::String,
            amount: ::ethers::core::types::U256,
        ) -> ::ethers::contract::builders::ContractCall<M, ()> {
            self.0
                .method_hash([242, 248, 158, 137], (acc, amount))
                .expect("method not found (this should never happen)")
        }
        ///Calls the contract's `getBalance` (0x3a51d246) function
        pub fn get_balance(
            &self,
            acc: ::std::string::String,
        ) -> ::ethers::contract::builders::ContractCall<M, ::ethers::core::types::U256> {
            self.0
                .method_hash([58, 81, 210, 70], acc)
                .expect("method not found (this should never happen)")
        }
        ///Calls the contract's `sendPayment` (0xca305435) function
        pub fn send_payment(
            &self,
            dest: ::std::string::String,
            src: ::std::string::String,
            amount: ::ethers::core::types::U256,
        ) -> ::ethers::contract::builders::ContractCall<M, ()> {
            self.0
                .method_hash([202, 48, 84, 53], (dest, src, amount))
                .expect("method not found (this should never happen)")
        }
        ///Calls the contract's `updateSaving` (0x0b488b37) function
        pub fn update_saving(
            &self,
            acc: ::std::string::String,
            amount: ::ethers::core::types::U256,
        ) -> ::ethers::contract::builders::ContractCall<M, ()> {
            self.0
                .method_hash([11, 72, 139, 55], (acc, amount))
                .expect("method not found (this should never happen)")
        }
        ///Calls the contract's `writeCheck` (0x0be8374d) function
        pub fn write_check(
            &self,
            acc: ::std::string::String,
            amount: ::ethers::core::types::U256,
        ) -> ::ethers::contract::builders::ContractCall<M, ()> {
            self.0
                .method_hash([11, 232, 55, 77], (acc, amount))
                .expect("method not found (this should never happen)")
        }
    }
    impl<M: ::ethers::providers::Middleware> From<::ethers::contract::Contract<M>>
    for SmallBank<M> {
        fn from(contract: ::ethers::contract::Contract<M>) -> Self {
            Self::new(contract.address(), contract.client())
        }
    }
    ///Container type for all input parameters for the `amalgamate` function with signature `amalgamate(string,string)` and selector `0x26328cdd`
    #[derive(
        Clone,
        ::ethers::contract::EthCall,
        ::ethers::contract::EthDisplay,
        Default,
        Debug,
        PartialEq,
        Eq,
        Hash
    )]
    #[ethcall(name = "amalgamate", abi = "amalgamate(string,string)")]
    pub struct AmalgamateCall {
        pub dest: ::std::string::String,
        pub src: ::std::string::String,
    }
    ///Container type for all input parameters for the `createAccount` function with signature `createAccount(string,uint256,uint256)` and selector `0x39d8d684`
    #[derive(
        Clone,
        ::ethers::contract::EthCall,
        ::ethers::contract::EthDisplay,
        Default,
        Debug,
        PartialEq,
        Eq,
        Hash
    )]
    #[ethcall(name = "createAccount", abi = "createAccount(string,uint256,uint256)")]
    pub struct CreateAccountCall {
        pub acc: ::std::string::String,
        pub init_check: ::ethers::core::types::U256,
        pub init_save: ::ethers::core::types::U256,
    }
    ///Container type for all input parameters for the `depositChecking` function with signature `depositChecking(string,uint256)` and selector `0xf2f89e89`
    #[derive(
        Clone,
        ::ethers::contract::EthCall,
        ::ethers::contract::EthDisplay,
        Default,
        Debug,
        PartialEq,
        Eq,
        Hash
    )]
    #[ethcall(name = "depositChecking", abi = "depositChecking(string,uint256)")]
    pub struct DepositCheckingCall {
        pub acc: ::std::string::String,
        pub amount: ::ethers::core::types::U256,
    }
    ///Container type for all input parameters for the `getBalance` function with signature `getBalance(string)` and selector `0x3a51d246`
    #[derive(
        Clone,
        ::ethers::contract::EthCall,
        ::ethers::contract::EthDisplay,
        Default,
        Debug,
        PartialEq,
        Eq,
        Hash
    )]
    #[ethcall(name = "getBalance", abi = "getBalance(string)")]
    pub struct GetBalanceCall {
        pub acc: ::std::string::String,
    }
    ///Container type for all input parameters for the `sendPayment` function with signature `sendPayment(string,string,uint256)` and selector `0xca305435`
    #[derive(
        Clone,
        ::ethers::contract::EthCall,
        ::ethers::contract::EthDisplay,
        Default,
        Debug,
        PartialEq,
        Eq,
        Hash
    )]
    #[ethcall(name = "sendPayment", abi = "sendPayment(string,string,uint256)")]
    pub struct SendPaymentCall {
        pub dest: ::std::string::String,
        pub src: ::std::string::String,
        pub amount: ::ethers::core::types::U256,
    }
    ///Container type for all input parameters for the `updateSaving` function with signature `updateSaving(string,uint256)` and selector `0x0b488b37`
    #[derive(
        Clone,
        ::ethers::contract::EthCall,
        ::ethers::contract::EthDisplay,
        Default,
        Debug,
        PartialEq,
        Eq,
        Hash
    )]
    #[ethcall(name = "updateSaving", abi = "updateSaving(string,uint256)")]
    pub struct UpdateSavingCall {
        pub acc: ::std::string::String,
        pub amount: ::ethers::core::types::U256,
    }
    ///Container type for all input parameters for the `writeCheck` function with signature `writeCheck(string,uint256)` and selector `0x0be8374d`
    #[derive(
        Clone,
        ::ethers::contract::EthCall,
        ::ethers::contract::EthDisplay,
        Default,
        Debug,
        PartialEq,
        Eq,
        Hash
    )]
    #[ethcall(name = "writeCheck", abi = "writeCheck(string,uint256)")]
    pub struct WriteCheckCall {
        pub acc: ::std::string::String,
        pub amount: ::ethers::core::types::U256,
    }
    ///Container type for all of the contract's call
    #[derive(Clone, ::ethers::contract::EthAbiType, Debug, PartialEq, Eq, Hash)]
    pub enum SmallBankCalls {
        Amalgamate(AmalgamateCall),
        CreateAccount(CreateAccountCall),
        DepositChecking(DepositCheckingCall),
        GetBalance(GetBalanceCall),
        SendPayment(SendPaymentCall),
        UpdateSaving(UpdateSavingCall),
        WriteCheck(WriteCheckCall),
    }
    impl ::ethers::core::abi::AbiDecode for SmallBankCalls {
        fn decode(
            data: impl AsRef<[u8]>,
        ) -> ::core::result::Result<Self, ::ethers::core::abi::AbiError> {
            let data = data.as_ref();
            if let Ok(decoded)
                = <AmalgamateCall as ::ethers::core::abi::AbiDecode>::decode(data) {
                return Ok(Self::Amalgamate(decoded));
            }
            if let Ok(decoded)
                = <CreateAccountCall as ::ethers::core::abi::AbiDecode>::decode(data) {
                return Ok(Self::CreateAccount(decoded));
            }
            if let Ok(decoded)
                = <DepositCheckingCall as ::ethers::core::abi::AbiDecode>::decode(data) {
                return Ok(Self::DepositChecking(decoded));
            }
            if let Ok(decoded)
                = <GetBalanceCall as ::ethers::core::abi::AbiDecode>::decode(data) {
                return Ok(Self::GetBalance(decoded));
            }
            if let Ok(decoded)
                = <SendPaymentCall as ::ethers::core::abi::AbiDecode>::decode(data) {
                return Ok(Self::SendPayment(decoded));
            }
            if let Ok(decoded)
                = <UpdateSavingCall as ::ethers::core::abi::AbiDecode>::decode(data) {
                return Ok(Self::UpdateSaving(decoded));
            }
            if let Ok(decoded)
                = <WriteCheckCall as ::ethers::core::abi::AbiDecode>::decode(data) {
                return Ok(Self::WriteCheck(decoded));
            }
            Err(::ethers::core::abi::Error::InvalidData.into())
        }
    }
    impl ::ethers::core::abi::AbiEncode for SmallBankCalls {
        fn encode(self) -> Vec<u8> {
            match self {
                Self::Amalgamate(element) => {
                    ::ethers::core::abi::AbiEncode::encode(element)
                }
                Self::CreateAccount(element) => {
                    ::ethers::core::abi::AbiEncode::encode(element)
                }
                Self::DepositChecking(element) => {
                    ::ethers::core::abi::AbiEncode::encode(element)
                }
                Self::GetBalance(element) => {
                    ::ethers::core::abi::AbiEncode::encode(element)
                }
                Self::SendPayment(element) => {
                    ::ethers::core::abi::AbiEncode::encode(element)
                }
                Self::UpdateSaving(element) => {
                    ::ethers::core::abi::AbiEncode::encode(element)
                }
                Self::WriteCheck(element) => {
                    ::ethers::core::abi::AbiEncode::encode(element)
                }
            }
        }
    }
    impl ::core::fmt::Display for SmallBankCalls {
        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
            match self {
                Self::Amalgamate(element) => ::core::fmt::Display::fmt(element, f),
                Self::CreateAccount(element) => ::core::fmt::Display::fmt(element, f),
                Self::DepositChecking(element) => ::core::fmt::Display::fmt(element, f),
                Self::GetBalance(element) => ::core::fmt::Display::fmt(element, f),
                Self::SendPayment(element) => ::core::fmt::Display::fmt(element, f),
                Self::UpdateSaving(element) => ::core::fmt::Display::fmt(element, f),
                Self::WriteCheck(element) => ::core::fmt::Display::fmt(element, f),
            }
        }
    }
    impl ::core::convert::From<AmalgamateCall> for SmallBankCalls {
        fn from(value: AmalgamateCall) -> Self {
            Self::Amalgamate(value)
        }
    }
    impl ::core::convert::From<CreateAccountCall> for SmallBankCalls {
        fn from(value: CreateAccountCall) -> Self {
            Self::CreateAccount(value)
        }
    }
    impl ::core::convert::From<DepositCheckingCall> for SmallBankCalls {
        fn from(value: DepositCheckingCall) -> Self {
            Self::DepositChecking(value)
        }
    }
    impl ::core::convert::From<GetBalanceCall> for SmallBankCalls {
        fn from(value: GetBalanceCall) -> Self {
            Self::GetBalance(value)
        }
    }
    impl ::core::convert::From<SendPaymentCall> for SmallBankCalls {
        fn from(value: SendPaymentCall) -> Self {
            Self::SendPayment(value)
        }
    }
    impl ::core::convert::From<UpdateSavingCall> for SmallBankCalls {
        fn from(value: UpdateSavingCall) -> Self {
            Self::UpdateSaving(value)
        }
    }
    impl ::core::convert::From<WriteCheckCall> for SmallBankCalls {
        fn from(value: WriteCheckCall) -> Self {
            Self::WriteCheck(value)
        }
    }
    ///Container type for all return fields from the `getBalance` function with signature `getBalance(string)` and selector `0x3a51d246`
    #[derive(
        Clone,
        ::ethers::contract::EthAbiType,
        ::ethers::contract::EthAbiCodec,
        Default,
        Debug,
        PartialEq,
        Eq,
        Hash
    )]
    pub struct GetBalanceReturn {
        pub balance: ::ethers::core::types::U256,
    }
}