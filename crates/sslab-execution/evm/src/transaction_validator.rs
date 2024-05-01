use async_trait::async_trait;
use narwhal_types::{Batch, BatchAPI};
use narwhal_worker::TransactionValidator;
use reth::primitives::{alloy_primitives::private::alloy_rlp::Decodable, TransactionSigned};
use sui_protocol_config::ProtocolConfig;

#[derive(Clone, Debug, Default)]
pub struct EthereumTxValidator;

#[async_trait]
impl TransactionValidator for EthereumTxValidator {
    type Error = eyre::Report;

    /// Determines if a transaction valid for the worker to consider putting in a batch
    fn validate(&self, t: &[u8]) -> Result<(), Self::Error> {
        let mut raw_tx = t;
        TransactionSigned::decode(&mut raw_tx)?;
        Ok(())
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
