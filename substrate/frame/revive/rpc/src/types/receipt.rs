//! Ethereum receipt types owned by the Ethereum RPC crate.

use super::Log;
use pallet_revive::evm::{Address, Byte, Bytes256, H256, TransactionInfo, TransactionSigned, U256};
use serde::{Deserialize, Serialize};
use sp_core::keccak_256;

/// Receipt information returned for an Ethereum transaction.
#[derive(Debug, Default, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ReceiptInfo {
	/// Actual blob gas price paid by the sender for EIP-4844 blob gas.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub blob_gas_price: Option<U256>,
	/// Amount of blob gas used by this EIP-4844 transaction.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub blob_gas_used: Option<U256>,
	/// Hash of the block containing the transaction.
	pub block_hash: H256,
	/// Number of the block containing the transaction.
	pub block_number: U256,
	/// Contract address created by the transaction, when it deployed code.
	pub contract_address: Option<Address>,
	/// Gas used by this transaction and all preceding transactions in the block.
	pub cumulative_gas_used: U256,
	/// Actual gas price paid by the sender after EIP-1559 fee resolution.
	pub effective_gas_price: U256,
	/// Sender address recovered from the transaction signature.
	pub from: Address,
	/// Gas used by this transaction.
	pub gas_used: U256,
	/// Logs emitted while executing this transaction.
	pub logs: Vec<Log>,
	/// Bloom filter over all logs emitted by this transaction.
	pub logs_bloom: Bytes256,
	/// Post-transaction state root for pre-Byzantium transactions.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub root: Option<H256>,
	/// Transaction execution status for Byzantium and newer transactions.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub status: Option<U256>,
	/// Receiver address, or `None` when the transaction deployed code.
	pub to: Option<Address>,
	/// Hash of the transaction this receipt belongs to.
	pub transaction_hash: H256,
	/// Index of the transaction within its block.
	pub transaction_index: U256,
	/// Typed transaction discriminator.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub r#type: Option<Byte>,
}

impl ReceiptInfo {
	/// Initialize a new receipt from extracted execution metadata.
	#[must_use]
	pub fn new(
		block_hash: H256,
		block_number: U256,
		contract_address: Option<Address>,
		from: Address,
		logs: Vec<Log>,
		to: Option<Address>,
		effective_gas_price: U256,
		gas_used: U256,
		success: bool,
		transaction_hash: H256,
		transaction_index: U256,
		r#type: Byte,
	) -> Self {
		let logs_bloom = Self::logs_bloom(&logs);
		ReceiptInfo {
			block_hash,
			block_number,
			contract_address,
			from,
			logs,
			logs_bloom,
			to,
			effective_gas_price,
			gas_used,
			status: Some(if success { U256::one() } else { U256::zero() }),
			transaction_hash,
			transaction_index,
			r#type: Some(r#type),
			..Default::default()
		}
	}

	/// Returns `true` when the transaction completed successfully.
	#[must_use]
	pub fn is_success(&self) -> bool {
		self.status.map_or(false, |status| status == U256::one())
	}

	/// Calculate the receipt logs bloom from the receipt logs.
	fn logs_bloom(logs: &[Log]) -> Bytes256 {
		let mut bloom = [0u8; 256];
		for log in logs {
			m3_2048(&mut bloom, log.address.as_ref());
			for topic in &log.topics {
				m3_2048(&mut bloom, topic.as_ref());
			}
		}
		bloom.into()
	}
}

/// Build pallet transaction information from an RPC receipt and signed transaction.
#[must_use]
pub(crate) fn transaction_info_from_receipt(
	receipt: &ReceiptInfo,
	transaction_signed: TransactionSigned,
) -> TransactionInfo {
	TransactionInfo {
		block_hash: receipt.block_hash,
		block_number: receipt.block_number,
		from: receipt.from,
		hash: receipt.transaction_hash,
		transaction_index: receipt.transaction_index,
		transaction_signed,
	}
}

/// Set the three bloom bits defined by the Ethereum receipt bloom algorithm.
///
/// See Section 4.4.1 "Transaction Receipt" of the Ethereum Yellow Paper.
fn m3_2048(bloom: &mut [u8; 256], bytes: &[u8]) {
	let hash = keccak_256(bytes);
	for i in [0, 2, 4] {
		let bit = (hash[i + 1] as usize + ((hash[i] as usize) << 8)) & 0x7FF;
		bloom[256 - 1 - bit / 8] |= 1 << (bit % 8);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use pallet_revive::evm::Bytes;

	#[test]
	fn logs_bloom_works() {
		let receipt: ReceiptInfo = serde_json::from_str(
		r#"
		{
			"blockHash": "0x835ee379aaabf4802a22a93ad8164c02bbdde2cc03d4552d5c642faf4e09d1f3",
			"blockNumber": "0x2",
			"contractAddress": null,
			"cumulativeGasUsed": "0x5d92",
			"effectiveGasPrice": "0x2dcd5c2d",
			"from": "0xb4f1f9ecfe5a28633a27f57300bda217e99b8969",
			"gasUsed": "0x5d92",
			"logs": [
				{
				"address": "0x82bdb002b9b1f36c42df15fbdc6886abcb2ab31d",
				"topics": [
					"0x1585375487296ff2f0370daeec4214074a032b31af827c12622fa9a58c16c7d0",
					"0x000000000000000000000000b4f1f9ecfe5a28633a27f57300bda217e99b8969"
				],
				"data": "0x00000000000000000000000000000000000000000000000000000000000030390000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000b48656c6c6f20776f726c64000000000000000000000000000000000000000000",
				"blockNumber": "0x2",
				"transactionHash": "0xad0075127962bdf73d787f2944bdb5f351876f23c35e6a48c1f5b6463a100af4",
				"transactionIndex": "0x0",
				"blockHash": "0x835ee379aaabf4802a22a93ad8164c02bbdde2cc03d4552d5c642faf4e09d1f3",
				"logIndex": "0x0",
				"removed": false
				}
			],
			"logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000400000008000000000000000000000000000000000000000000000000800000000040000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000004000000000000000800000000000000000080000000000000000000000000000000000000000000",
			"status": "0x1",
			"to": "0x82bdb002b9b1f36c42df15fbdc6886abcb2ab31d",
			"transactionHash": "0xad0075127962bdf73d787f2944bdb5f351876f23c35e6a48c1f5b6463a100af4",
			"transactionIndex": "0x0",
			"type": "0x2"
		}
		"#,
	)
	.unwrap();
		assert_eq!(receipt.logs_bloom, ReceiptInfo::logs_bloom(&receipt.logs));
	}

	#[test]
	fn deserializes_receipt_logs_and_bloom() {
		// Arrange
		let log = Log {
			address: Address::from([0x82; 20]),
			block_hash: H256::from([0x83; 32]),
			block_number: U256::from(2),
			data: Some(Bytes(vec![0xde, 0xad, 0xbe, 0xef])),
			log_index: U256::zero(),
			topics: vec![H256::from([0x15; 32]), H256::from([0xb4; 32])],
			transaction_hash: H256::from([0xad; 32]),
			transaction_index: U256::zero(),
			removed: false,
		};
		let logs_bloom = ReceiptInfo::logs_bloom(core::slice::from_ref(&log));
		let value = serde_json::json!({
			"blockHash": H256::from([0x83; 32]),
			"blockNumber": U256::from(2),
			"cumulativeGasUsed": U256::from(23_954),
			"effectiveGasPrice": U256::from(769_481_773),
			"from": Address::from([0xb4; 20]),
			"gasUsed": U256::from(23_954),
			"logs": [log.clone()],
			"logsBloom": logs_bloom,
			"status": U256::one(),
			"to": Address::from([0x82; 20]),
			"transactionHash": H256::from([0xad; 32]),
			"transactionIndex": U256::zero(),
			"type": Byte::from(2),
		});

		// Act
		let receipt = serde_json::from_value::<ReceiptInfo>(value).unwrap();

		// Assert
		assert_eq!(receipt.logs, vec![log]);
		assert_eq!(receipt.logs_bloom, logs_bloom);
	}
}
