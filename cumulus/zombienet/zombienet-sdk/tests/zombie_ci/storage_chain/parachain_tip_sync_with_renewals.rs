// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Tip-sync-with-renewals integration test.
//!
//! # What this proves
//!
//! 1. A node that warp-syncs to the tip starts with an EMPTY TRANSACTION column — none of the
//!    indexed-transaction blobs from before the warp target are on disk.
//! 2. When the collator submits a `transaction_storage::renew(block, index)` for a pre-warp entry,
//!    the renew block reaches the syncing node via tip sync. The block body references a
//!    `content_hash` the syncing node does not have. [`StorageChainBlockImport`] detects this,
//!    issues a bitswap `WANT-BLOCK`, receives the bytes from the collator, and writes them to the
//!    TRANSACTION column atomically with the block's BODY_INDEX entry.
//! 3. After the renew is finalized, the syncing node's `bitswap_v1_get` RPC returns the original
//!    blob — direct evidence the wrapper's fetch path ran AND the bytes landed in the TRANSACTION
//!    column.
//!
//! # Snapshot fixtures required
//!
//! This test loads a 300-block parachain snapshot and manifest produced by
//! `parachain_generate_db.rs`. CI will use GCS fixture URLs; until those buckets exist,
//! local runs should point the fixture env vars at generated files:
//!
//! ```bash
//! TARGET_BLOCKS=300 \
//! DB_OUTPUT_DIR=cumulus/zombienet/zombienet-sdk/tests/zombie_ci/storage_chain/fixtures/test-databases \
//! ZOMBIE_PROVIDER=native \
//!   cargo test --release -p cumulus-zombienet-sdk-tests \
//!     --features "zombie-ci generate-snapshots" \
//!     -- parachain_generate_databases --nocapture
//!
//! STORAGE_CHAIN_TIP_SYNC_SNAPSHOT=.../tip-sync-300.tgz \
//! STORAGE_CHAIN_RELAY_SNAPSHOT=.../relay.tgz \
//! STORAGE_CHAIN_TIP_SYNC_MANIFEST=.../tip-sync-300-manifest.json \
//! ZOMBIE_PROVIDER=native \
//!   cargo test --release -p cumulus-zombienet-sdk-tests \
//!     --features zombie-ci \
//!     -- parachain_tip_sync_with_renewals_test --nocapture
//! ```
//!
//! # Why the renew target comes from the manifest
//!
//! The stored blob identity is deterministic, but the exact block/index of the
//! latest generated renewal is captured from chain events while building the
//! snapshot. The test consumes that manifest directly instead of probing blocks.

use super::utils::{
	bitswap_v1_get, blake2_256,
	build_parachain_network_config_three_relay_validators_with_snapshots, expect_dont_have,
	expect_no_log_line, get_best_block_height, hash_to_cid, initialize_network,
	renew_data_with_hash, renewable_entry_data, verify_parachain_binaries,
	verify_warp_sync_completed, wait_for_block_height, wait_for_finalized_height,
	wait_for_fullnode, wait_for_new_block_beyond, wait_for_relay_chain_to_sync,
	wait_for_session_change_on_node, RenewableEntryManifest, ResolvedSnapshots,
	BLOCK_PRODUCTION_TIMEOUT_SECS, FIXTURE_RETENTION_PERIOD, NETWORK_READY_TIMEOUT_SECS,
	NODE_LOG_CONFIG, PARACHAIN_BINARY, PARA_ID, SNAPSHOT_STORE_INTERVAL, SYNC_TIMEOUT_SECS,
	TIP_SYNC_RENEWABLE_STORE_COUNT, TIP_SYNC_TARGET_BLOCKS,
};
use crate::test_log;
use anyhow::{anyhow, Context, Result};
use env_logger::Env;
use std::time::Duration;
use zombienet_orchestrator::AddCollatorOptions;
use zombienet_sdk::subxt::{config::substrate::SubstrateConfig, OnlineClient};

// Test parameters
const N_RENEW_EXERCISES: u64 = 5; // <= snapshot's RENEWABLE_STORE_COUNT (10)
const WARP_PRUNING_BLOCKS: u32 = 100;
const SESSION_CHANGE_TIMEOUT_SECS: u64 = 300;
const BITSWAP_RPC_POLL_TIMEOUT_SECS: u64 = 60;

fn manifest_content_hash(entry: &RenewableEntryManifest) -> Result<[u8; 32]> {
	let mut hash = [0u8; 32];
	hex::decode_to_slice(&entry.content_hash, &mut hash)
		.with_context(|| format!("invalid content_hash for manifest entry {}", entry.entry))?;
	Ok(hash)
}

#[tokio::test(flavor = "multi_thread")]
async fn parachain_tip_sync_with_renewals_test() -> Result<()> {
	const TEST: &str = "para_tip_sync_renewals";
	let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info")).try_init();

	verify_parachain_binaries()?;
	let snaps = ResolvedSnapshots::load()?;
	let manifest = snaps.load_manifest()?;
	anyhow::ensure!(
		manifest.target_blocks == TIP_SYNC_TARGET_BLOCKS,
		"unexpected manifest target_blocks: got {}, expected {}",
		manifest.target_blocks,
		TIP_SYNC_TARGET_BLOCKS,
	);
	anyhow::ensure!(
		manifest.store_interval == SNAPSHOT_STORE_INTERVAL,
		"unexpected manifest store_interval: got {}, expected {}",
		manifest.store_interval,
		SNAPSHOT_STORE_INTERVAL,
	);
	anyhow::ensure!(
		manifest.retention_period == FIXTURE_RETENTION_PERIOD,
		"unexpected manifest retention_period: got {}, expected {}",
		manifest.retention_period,
		FIXTURE_RETENTION_PERIOD,
	);
	anyhow::ensure!(
		manifest.renewable_store_count == TIP_SYNC_RENEWABLE_STORE_COUNT,
		"unexpected manifest renewable_store_count: got {}, expected {}",
		manifest.renewable_store_count,
		TIP_SYNC_RENEWABLE_STORE_COUNT,
	);
	anyhow::ensure!(
		manifest.entries.len() >= N_RENEW_EXERCISES as usize,
		"manifest has only {} renewable entries, need {}",
		manifest.entries.len(),
		N_RENEW_EXERCISES,
	);
	let renew_targets =
		manifest.entries.iter().take(N_RENEW_EXERCISES as usize).collect::<Vec<_>>();

	test_log!(TEST, "Loaded snapshot fixtures from disk");

	// ─────────────────────────────────────────────────────────────────────────
	// Phase 1: collator boots from 300-block snapshot, chain advances past it.
	// ─────────────────────────────────────────────────────────────────────────
	let config = build_parachain_network_config_three_relay_validators_with_snapshots(
		vec!["--ipfs-server".into(), NODE_LOG_CONFIG.into()],
		Some(snaps.as_parachain_snapshots()),
	)?;
	let mut network = initialize_network(config).await?;
	network.wait_until_is_up(NETWORK_READY_TIMEOUT_SECS).await?;

	{
		let alice = network.get_node("alice")?;
		wait_for_session_change_on_node(alice, SESSION_CHANGE_TIMEOUT_SECS).await?;

		let collator1 = network.get_node("collator-1")?;
		let snapshot_height = get_best_block_height(collator1).await?;
		test_log!(TEST, "Collator booted at block {} (from snapshot)", snapshot_height);
		wait_for_new_block_beyond(collator1, snapshot_height, BLOCK_PRODUCTION_TIMEOUT_SECS)
			.await?;
		test_log!(TEST, "Collator extended chain past snapshot tip");
	}

	// ─────────────────────────────────────────────────────────────────────────
	// Phase 2: warp-sync a fresh node.
	// ─────────────────────────────────────────────────────────────────────────
	network
		.add_collator(
			"sync-node",
			AddCollatorOptions {
				command: Some(PARACHAIN_BINARY.try_into()?),
				args: vec![
					"--sync=warp".into(),
					"--ipfs-server".into(),
					format!("--blocks-pruning={WARP_PRUNING_BLOCKS}").as_str().into(),
					NODE_LOG_CONFIG.into(),
				],
				is_validator: false,
				..Default::default()
			},
			PARA_ID,
		)
		.await?;

	let collator1 = network.get_node("collator-1")?;
	let sync_node = network.get_node("sync-node")?;
	wait_for_fullnode(sync_node).await?;
	wait_for_relay_chain_to_sync(sync_node, SYNC_TIMEOUT_SECS).await?;

	let warp_target = get_best_block_height(collator1).await?;
	wait_for_block_height(sync_node, warp_target, SYNC_TIMEOUT_SECS).await?;
	verify_warp_sync_completed(sync_node).await?;
	test_log!(TEST, "Sync-node warp-synced to block {}", warp_target);

	// ─────────────────────────────────────────────────────────────────────────
	// Phase 3: sanity — sync-node has NO pre-warp snapshot data.
	// ─────────────────────────────────────────────────────────────────────────
	for entry in &renew_targets {
		let data = renewable_entry_data(entry.entry);
		let expected_hash = manifest_content_hash(entry)?;
		anyhow::ensure!(
			blake2_256(&data) == expected_hash,
			"manifest hash does not match deterministic data for entry {}",
			entry.entry,
		);
		let cid = hash_to_cid(&blake2_256(&data));
		anyhow::ensure!(
			cid == entry.cid,
			"manifest CID does not match deterministic data for entry {}",
			entry.entry,
		);
		expect_dont_have(sync_node, &cid, Duration::from_secs(BITSWAP_RPC_POLL_TIMEOUT_SECS))
			.await
			.with_context(|| {
				format!("pre-renewal: sync-node should not have entry {} ({cid})", entry.entry)
			})?;
	}
	test_log!(
		TEST,
		"Confirmed sync-node lacks all {N_RENEW_EXERCISES} pre-warp entries (DontHave)"
	);

	// ─────────────────────────────────────────────────────────────────────────
	// Phase 4: continuous renewals — the core of the test.
	//
	// `pallet_transaction_storage::renew(block, index)` requires the entry to
	// still exist at (block, index). The snapshot generator records the latest
	// valid renewal targets in the fixture manifest, so the test renews those
	// targets directly instead of probing historical blocks.
	// ─────────────────────────────────────────────────────────────────────────
	let collator_client: OnlineClient<SubstrateConfig> = collator1.wait_client().await?;
	let mut bob_nonce = collator_client
		.tx()
		.account_nonce(
			&zombienet_sdk::subxt_signer::sr25519::dev::bob().public_key().to_account_id(),
		)
		.await?;

	let mut renewed_hashes = Vec::new();

	for (i, entry) in renew_targets.iter().enumerate() {
		let expected_hash = manifest_content_hash(entry)?;
		let outcome = renew_data_with_hash(
			&collator_client,
			entry.latest_renewal_block,
			entry.latest_renewal_index,
			bob_nonce,
		)
		.await
		.with_context(|| {
			format!(
				"renewing manifest entry {} at block {}, index {}",
				entry.entry, entry.latest_renewal_block, entry.latest_renewal_index,
			)
		})?;
		anyhow::ensure!(
			outcome.content_hash == expected_hash,
			"renewed content hash mismatch for manifest entry {}",
			entry.entry,
		);
		let renew_block = outcome.renewed_at_block;
		test_log!(
			TEST,
			"✓ Renew {}/{}: entry={}, block={}, index={} → renewed at block {} index {}",
			i + 1,
			N_RENEW_EXERCISES,
			entry.entry,
			entry.latest_renewal_block,
			entry.latest_renewal_index,
			renew_block,
			outcome.renewed_index,
		);
		bob_nonce += 1;
		renewed_hashes.push(expected_hash);

		wait_for_finalized_height(collator1, renew_block, BLOCK_PRODUCTION_TIMEOUT_SECS).await?;
		wait_for_block_height(sync_node, renew_block, SYNC_TIMEOUT_SECS).await?;
	}

	let deadline = std::time::Instant::now() + Duration::from_secs(BITSWAP_RPC_POLL_TIMEOUT_SECS);
	let renewed_entries_available = loop {
		let mut renewed_entries_available = 0usize;
		for content_hash in &renewed_hashes {
			let cid = hash_to_cid(content_hash);
			if matches!(bitswap_v1_get(sync_node, &cid).await, Ok(Some(bytes)) if blake2_256(&bytes) == *content_hash)
			{
				renewed_entries_available += 1;
			}
		}

		if renewed_entries_available >= N_RENEW_EXERCISES as usize {
			break renewed_entries_available;
		}

		if std::time::Instant::now() >= deadline {
			return Err(anyhow!(
				"post-renewal: sync-node served only {} of {} renewed entries within {}s",
				renewed_entries_available,
				renewed_hashes.len(),
				BITSWAP_RPC_POLL_TIMEOUT_SECS,
			));
		}

		tokio::time::sleep(Duration::from_secs(1)).await;
	};
	test_log!(
		TEST,
		"✓ Sync-node serves {} renewed pre-warp entries via bitswap_v1_get",
		renewed_entries_available,
	);

	// ─────────────────────────────────────────────────────────────────────────
	// Phase 5: negative log assertions — no hash mismatches anywhere.
	// ─────────────────────────────────────────────────────────────────────────
	expect_no_log_line(
		collator1,
		"(?i)bitswap.*hash.mismatch",
		10,
		"collator logged a bitswap hash mismatch",
	)
	.await?;
	expect_no_log_line(
		sync_node,
		"(?i)bitswap.*hash.mismatch",
		10,
		"sync-node logged a bitswap hash mismatch",
	)
	.await?;

	test_log!(TEST, "=== parachain_tip_sync_with_renewals PASSED ===");
	network.destroy().await?;
	Ok(())
}
