use async_trait::async_trait;
use ethers_core::{utils::rlp::{Rlp, Decodable, DecoderError}, types::{Transaction, SignatureError}};
use narwhal_types::{Batch, BatchAPI};
use narwhal_worker::TransactionValidator;
use sui_protocol_config::ProtocolConfig;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TxValidationError {
    #[error(transparent)]
    SignatureError(#[from] SignatureError),
    #[error(transparent)]
    DecoderError(#[from] DecoderError),
}


#[derive(Clone, Debug, Default)]
pub struct EthereumTxValidator;

#[async_trait]
impl TransactionValidator for EthereumTxValidator {

    type Error = TxValidationError;

    /// Determines if a transaction valid for the worker to consider putting in a batch
    fn validate(&self, t: &[u8]) -> Result<(), Self::Error> {

        let rlp = Rlp::new(t);
        
        match Transaction::decode(&rlp) {
            Ok(tx) => {
                let _ = tx.recover_from()?;
                Ok(())
            },
            Err(e) => Err(TxValidationError::DecoderError(e))
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