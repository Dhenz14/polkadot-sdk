// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Subxt transaction helpers: nonce management, storage operations, retention period.

#[cfg(feature = "generate-snapshots")]
use super::{config::TRANSACTION_TIMEOUT_SECS, crypto::retention_period_storage_key};
use anyhow::{Result, anyhow};
use codec::Decode;
use std::time::Duration;
use zombienet_sdk::{
	subxt::{
		OnlineClient,
		config::substrate::{SubstrateConfig, SubstrateExtrinsicParamsBuilder},
		dynamic::{Value, tx},
	},
	subxt_signer::sr25519::dev,
};

pub struct RenewOutcome {
	pub renewed_at_block: u64,
	pub content_hash: [u8; 32],
}

fn renewed_content_hash(
	events: &zombienet_sdk::subxt::blocks::ExtrinsicEvents<SubstrateConfig>,
) -> Result<[u8; 32]> {
	for event in events.iter() {
		let event = event?;
		if event.pallet_name() == "TransactionStorage" && event.variant_name() == "Renewed" {
			let (_index, content_hash): (u32, [u8; 32]) =
				Decode::decode(&mut &event.field_bytes()[..])?;
			return Ok(content_hash);
		}
	}

	anyhow::bail!("Renewed event not found in extrinsic events")
}

#[cfg(feature = "generate-snapshots")]
pub async fn wait_for_in_best_block(
	mut progress: zombienet_sdk::subxt::tx::TxProgress<
		SubstrateConfig,
		OnlineClient<SubstrateConfig>,
	>,
) -> Result<(
	zombienet_sdk::subxt::utils::H256,
	zombienet_sdk::subxt::blocks::ExtrinsicEvents<SubstrateConfig>,
)> {
	use zombienet_sdk::subxt::tx::TxStatus;

	while let Some(status) = progress.next().await {
		match status? {
			TxStatus::InBestBlock(tx_in_block) => {
				let block_hash = tx_in_block.block_hash();
				let events = tx_in_block.wait_for_success().await?;
				return Ok((block_hash, events));
			},
			TxStatus::Error { message } |
			TxStatus::Invalid { message } |
			TxStatus::Dropped { message } => {
				anyhow::bail!("Transaction failed: {}", message);
			},
			_ => continue,
		}
	}
	anyhow::bail!("Transaction stream ended without InBestBlock status")
}

/// Use for LDB tests where database state must be consistent.
pub async fn wait_for_finalized(
	mut progress: zombienet_sdk::subxt::tx::TxProgress<
		SubstrateConfig,
		OnlineClient<SubstrateConfig>,
	>,
) -> Result<(
	zombienet_sdk::subxt::utils::H256,
	zombienet_sdk::subxt::blocks::ExtrinsicEvents<SubstrateConfig>,
)> {
	use zombienet_sdk::subxt::tx::TxStatus;

	while let Some(status) = progress.next().await {
		match status? {
			TxStatus::InFinalizedBlock(tx_in_block) => {
				let block_hash = tx_in_block.block_hash();
				let events = tx_in_block.wait_for_success().await?;
				return Ok((block_hash, events));
			},
			TxStatus::Error { message } |
			TxStatus::Invalid { message } |
			TxStatus::Dropped { message } => {
				anyhow::bail!("Transaction failed: {}", message);
			},
			_ => continue,
		}
	}
	anyhow::bail!("Transaction stream ended without InFinalizedBlock status")
}

/// Returns (block_number, next_nonce). Waits for best block.
#[cfg(feature = "generate-snapshots")]
pub async fn set_retention_period(
	client: &OnlineClient<SubstrateConfig>,
	retention_period: u32,
	nonce: u64,
) -> Result<()> {
	let signer = dev::alice();
	let key = retention_period_storage_key();
	let value_bytes = retention_period.to_le_bytes().to_vec();

	log::info!(
		"Setting RetentionPeriod to {} blocks via sudo (key: 0x{}, value: 0x{})",
		retention_period,
		hex::encode(&key),
		hex::encode(&value_bytes)
	);

	let items = Value::unnamed_composite([Value::unnamed_composite([
		Value::from_bytes(&key),
		Value::from_bytes(&value_bytes),
	])]);

	let set_storage_call = tx("System", "set_storage", vec![items]);
	let sudo_call = tx("Sudo", "sudo", vec![set_storage_call.into_value()]);
	let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();

	tokio::time::timeout(Duration::from_secs(TRANSACTION_TIMEOUT_SECS), async {
		let progress = client.tx().sign_and_submit_then_watch(&sudo_call, &signer, params).await?;
		wait_for_in_best_block(progress).await?;
		Ok::<_, anyhow::Error>(())
	})
	.await
	.map_err(|_| anyhow!("set_retention_period transaction timed out"))??;

	log::info!("RetentionPeriod set successfully");
	Ok(())
}

#[cfg(feature = "generate-snapshots")]
pub async fn get_alice_nonce(node: &zombienet_sdk::NetworkNode) -> Result<u64> {
	let client: OnlineClient<SubstrateConfig> = node.wait_client().await?;
	let alice_account_id = dev::alice().public_key().to_account_id();
	let nonce = client.tx().account_nonce(&alice_account_id).await?;
	log::info!("Alice's current nonce: {}", nonce);
	Ok(nonce)
}

pub async fn renew_data_with_hash(
	client: &OnlineClient<SubstrateConfig>,
	block: u64,
	index: u32,
	nonce: u64,
) -> Result<RenewOutcome> {
	let signer = dev::bob();
	let renew_call = tx(
		"TransactionStorage",
		"renew",
		vec![Value::u128(block as u128), Value::u128(index as u128)],
	);
	log::info!("Renew (bob): nonce={}, block={}, index={}", nonce, block, index);
	let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).immortal().build();

	let (block_hash, _events) = tokio::time::timeout(Duration::from_secs(120), async {
		let progress = client.tx().sign_and_submit_then_watch(&renew_call, &signer, params).await?;
		wait_for_finalized(progress).await
	})
	.await
	.map_err(|_| {
		anyhow!("renew transaction timed out (block={}, index={}, nonce={})", block, index, nonce)
	})??;

	let content_hash = renewed_content_hash(&_events)?;
	let b = client.blocks().at(block_hash).await?;
	log::info!(
		"Renew included at block {} (renewed entry from block {}, index {})",
		b.number(),
		block,
		index
	);
	Ok(RenewOutcome { renewed_at_block: b.number() as u64, content_hash })
}

#[cfg(feature = "generate-snapshots")]
pub async fn renew_data(
	client: &OnlineClient<SubstrateConfig>,
	block: u64,
	index: u32,
	nonce: u64,
) -> Result<u64> {
	Ok(renew_data_with_hash(client, block, index, nonce).await?.renewed_at_block)
}
