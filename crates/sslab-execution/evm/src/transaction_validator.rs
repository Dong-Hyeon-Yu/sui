use async_trait::async_trait;
use ethers_core::{utils::rlp::Rlp, types::{SignatureError, transaction::eip2718::{TypedTransaction, TypedTransactionError}}};
use narwhal_types::{Batch, BatchAPI};
use narwhal_worker::TransactionValidator;
use sui_protocol_config::ProtocolConfig;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TxValidationError {
    #[error(transparent)]
    SignatureError(#[from] SignatureError),
    #[error(transparent)]
    DecoderError(#[from] TypedTransactionError),
    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),
}

#[derive(Clone, Debug, Default)]
pub struct EthereumTxValidator;

#[async_trait]
impl TransactionValidator for EthereumTxValidator {

    type Error = TxValidationError;

    /// Determines if a transaction valid for the worker to consider putting in a batch
    fn validate(&self, t: &[u8]) -> Result<(), Self::Error> { 

        match serde_json::from_slice::<TypedTransaction>(t) {
            Ok(_) => Ok(()),
            Err(_) => {
                let rlp = Rlp::new(t);
        
                match TypedTransaction::decode_signed(&rlp) {
                    Ok((tx, sig)) => {
                        if let Err(e) = sig.verify(tx.sighash(), *tx.from().unwrap()) {
                            return Err(TxValidationError::SignatureError(e));
                        }
                        // debug!("validated tx: {:?}, sig: {:?}", tx, sig);
                        Ok(())
                    },
                    Err(e) => Err(TxValidationError::DecoderError(e))
                }
            }
        }
    }

    /// Determines if this batch can be voted on
    async fn validate_batch(
        &self,
        b: &Batch,
        _protocol_config: &ProtocolConfig,
    ) -> Result<(), Self::Error> {
        for t in b.transactions() {
            self.validate(t)?;
        }

        Ok(())
    }
}