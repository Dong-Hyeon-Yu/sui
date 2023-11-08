use std::{sync::Arc, str::FromStr};

use ethers_core::{
    types::{TransactionRequest, Signature, Address, H256, U256, transaction::eip2718::TypedTransaction, H160}, 
    rand::{distributions::Uniform, self, prelude::Distribution}, 
    utils::{rlp, hex}
};
use ethers_providers::{Provider, Http};
use ethers_signers::{LocalWallet, Signer};
use narwhal_types::{TransactionsClient, TransactionProto, Empty};
use sha3::{Keccak256, Digest};
use sui_network::tonic::{transport::Channel, self};
use tracing::{info, warn};

use crate::workloads::smallbank::contract::SmallBank;
// use crate::SMALLBANK_BYTECODE;


pub const CONTRACT_BYTECODE: &str = include_str!("../../contracts/SmallBank.bin");
pub const ADMIN_SECRET_KEY: &[u8] = &[95 as u8, 126, 251, 131, 73, 90, 235, 201, 21, 22, 203, 137, 149, 240, 205, 60, 221, 27, 81, 53, 2, 200, 90, 185, 25, 240, 166, 21, 177, 41, 49, 254];
// pub const ADMIN_ADDRESS: &str = "0xe14de1592b52481b94b99df4e9653654e14fffb6";
#[allow(dead_code)]
pub const DEFAULT_CONTRACT_ADDRESS: &str = "0x1000000000000000000000000000000000000000";
pub const DEFAULT_CHAIN_ID: u64 = 9;  // ISTANBUL 

pub enum SmallBankTransactionType {
    AMALGAMATE,
    GetBalance,
    SendPayment,
    UpdateBalance,
    UpdateSaving,
    WriteCheck,
}


impl SmallBankTransactionType {
    pub fn from(value: u32) -> SmallBankTransactionType {
        match value {
            0 => SmallBankTransactionType::AMALGAMATE,
            1 => SmallBankTransactionType::GetBalance,
            2 => SmallBankTransactionType::SendPayment,
            3 => SmallBankTransactionType::UpdateBalance,
            4 => SmallBankTransactionType::UpdateSaving,
            5 => SmallBankTransactionType::WriteCheck,
            _ => panic!("Invalid transaction type"),
        }
    }
}

#[derive(Clone)]
pub struct SmallBankTransactionHandler {
    op_gen: Uniform<u32>,
    nonce_gen: Uniform<u64>,
    uniform_acc_gen: Uniform<u32>,
    uniform_bal_gen: Uniform<u32>,
    admin_wallet: LocalWallet,
    provider: Provider<Http>,
    chain_id: u64,
    narwhal_client: TransactionsClient<Channel>,
    contract: Option<SmallBank<Provider<Http>>>,
}

impl SmallBankTransactionHandler {
    pub fn new(provider: Provider<Http>, narwhal_client: TransactionsClient<Channel>, chain_id: u64) -> SmallBankTransactionHandler {
        let nonce_gen = Uniform::new(u64::MIN, u64::MAX);

        warn!("contract address: {}", H160::from_str(DEFAULT_CONTRACT_ADDRESS).unwrap());
        SmallBankTransactionHandler {
            op_gen: Uniform::new(0, 6),
            nonce_gen,
            uniform_acc_gen: Uniform::new(1, 100000),
            uniform_bal_gen: Uniform::new(1, 10),
            admin_wallet: LocalWallet::from_bytes(ADMIN_SECRET_KEY.try_into().unwrap()).unwrap().with_chain_id(chain_id),
            provider: provider.clone(),
            chain_id,
            narwhal_client,
            contract: Some(SmallBank::new(H160::from_str(DEFAULT_CONTRACT_ADDRESS).unwrap(), Arc::new(provider))),
        }
    }

    #[allow(dead_code)]
    pub async fn init(&mut self) -> Result<tonic::Response<Empty>, tonic::Status> {
        info!("Init smallbank transaction handler");
        info!("admin address: {:?}", self.admin_wallet.address());
        
        let nonce = self.register_admin_account().await;
        self.contract = Some(SmallBank::new(self.create_contract_address(&nonce), Arc::new(self.provider.clone())));
        self.deploy_contract().await
    }

    #[allow(dead_code)]
    async fn submit_transaction(&mut self, tx_request: TransactionRequest) -> Result<tonic::Response<Empty>, tonic::Status> {
        let tx: TypedTransaction = From::<TransactionRequest>::from(tx_request);
        let raw_tx = self.get_signed(tx);
            
        // NOTE: This log entry is used to compute performance.
        let tx_id = u64::from_be_bytes(raw_tx[2..10].try_into().unwrap());
        info!("Sending sample transaction {tx_id}");
        
        self.narwhal_client.submit_transaction(TransactionProto { transaction: raw_tx }).await
    }

    #[allow(dead_code)]
    async fn deploy_contract(&mut self) -> Result<tonic::Response<Empty>, tonic::Status> {
        info!("Deploy contract");

        let tx_request = TransactionRequest::default()
            .from(self.admin_wallet.address())
            .chain_id(self.chain_id)
            // .value(1_000_000 as i32)
            .gas(u64::MAX)
            .data(hex::decode(CONTRACT_BYTECODE).unwrap())
            .nonce(U256::one());

        self.submit_transaction(tx_request).await
    }

    #[allow(dead_code)]
    fn create_contract_address(&mut self, admin_nonce: &U256) -> Address {
        let mut stream = rlp::RlpStream::new_list(2);
        stream.append(&self.admin_wallet.address());
        stream.append(admin_nonce);
        H256::from_slice(Keccak256::digest(&stream.out()).as_slice()).into()
    }

    #[allow(dead_code)]
    async fn register_admin_account(&mut self) -> U256 {
        info!("Register admin account");

        let nonce = U256::one();

        let tx_request = TransactionRequest::default()
            .from(self.admin_wallet.address())
            .value(U256::MAX)
            .chain_id(self.chain_id)
            .nonce(nonce);

        self.submit_transaction(tx_request).await.expect("failed to register admin account");

        nonce.into()
    }

    pub fn create_random_request(&self) -> bytes::Bytes {
        self.create_request(self.get_random_op())
    }

    fn create_request(&self, ops: SmallBankTransactionType) -> bytes::Bytes {
        let mut tx = match ops {
            SmallBankTransactionType::AMALGAMATE => {
                self.contract.as_ref().unwrap().almagate(
                    self.get_random_account_id(),
                    self.get_random_account_id(),
                ).tx
            },
            SmallBankTransactionType::GetBalance => {
                self.contract.as_ref().unwrap().get_balance(
                    self.get_random_account_id(),
                ).tx
            },
            SmallBankTransactionType::SendPayment => {
                self.contract.as_ref().unwrap().send_payment(
                    self.get_random_account_id(),
                    self.get_random_account_id(),
                    self.get_random_balance()
                ).tx
            },
            SmallBankTransactionType::UpdateBalance => {
                self.contract.as_ref().unwrap().update_balance(
                    self.get_random_account_id(),
                    self.get_random_balance()
                ).tx
            },
            SmallBankTransactionType::UpdateSaving => {
                self.contract.as_ref().unwrap().update_saving(
                    self.get_random_account_id(),
                    self.get_random_balance()
                ).tx
            },
            SmallBankTransactionType::WriteCheck => {
                self.contract.as_ref().unwrap().write_check(
                    self.get_random_account_id(),
                    self.get_random_balance()
                ).tx
            }
        };

        tx.set_from(self.admin_wallet.address())
            .set_to(H160::from_str(DEFAULT_CONTRACT_ADDRESS).unwrap())
            .set_chain_id(self.chain_id)
            .set_nonce(self.get_random_nonce())
            .set_gas(u64::MAX)
            .set_gas_price(U256::zero());

        self.get_signed(tx)
    }

    fn get_random_op(&self) -> SmallBankTransactionType {
        SmallBankTransactionType::from(self.op_gen.sample(&mut rand::thread_rng()))
    }

    fn get_random_account_id(&self) -> String {
        self.uniform_acc_gen.sample(&mut rand::thread_rng()).to_string()
    }

    fn get_random_balance(&self) -> U256 {
        U256::from(self.uniform_bal_gen.sample(&mut rand::thread_rng()))
    }

    fn get_random_nonce(&self) -> U256 {
        U256::from(self.nonce_gen.sample(&mut rand::thread_rng()))
    }

    fn get_signed(&self, tx: TypedTransaction) -> bytes::Bytes {
        let signature: Signature = self.admin_wallet.sign_transaction_sync(&tx).expect("signature failed");
        tx.rlp_signed(&signature).0
    }
}


