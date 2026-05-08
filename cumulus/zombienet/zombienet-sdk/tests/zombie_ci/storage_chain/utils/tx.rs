// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Subxt transaction helpers: nonce management and storage operations.

use anyhow::{anyhow, Result};
use codec::Decode;
use std::time::Duration;
use zombienet_sdk::{
	subxt::{
		config::substrate::{SubstrateConfig, SubstrateExtrinsicParamsBuilder},
		dynamic::{tx, Value},
		OnlineClient,
	},
	subxt_signer::sr25519::dev,
};

pub struct RenewOutcome {
	pub renewed_at_block: u64,
	pub renewed_index: u32,
	pub content_hash: [u8; 32],
}

fn renewed_content_hash(
	events: &zombienet_sdk::subxt::blocks::ExtrinsicEvents<SubstrateConfig>,
) -> Result<(u32, [u8; 32])> {
	for event in events.iter() {
		let event = event?;
		if event.pallet_name() == "TransactionStorage" && event.variant_name() == "Renewed" {
			let (index, content_hash): (u32, [u8; 32]) =
				Decode::decode(&mut &event.field_bytes()[..])?;
			return Ok((index, content_hash));
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

	let (renewed_index, content_hash) = renewed_content_hash(&_events)?;
	let b = client.blocks().at(block_hash).await?;
	log::info!(
		"Renew included at block {} index {} (renewed entry from block {}, index {})",
		b.number(),
		renewed_index,
		block,
		index
	);
	Ok(RenewOutcome { renewed_at_block: b.number() as u64, renewed_index, content_hash })
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
