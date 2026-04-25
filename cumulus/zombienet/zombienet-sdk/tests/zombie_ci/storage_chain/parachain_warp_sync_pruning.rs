// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

use super::utils::{
	authorize_and_store_data, build_parachain_network_config_three_relay_validators_with_snapshots,
	content_hash_and_cid, generate_test_data, get_alice_nonce, get_best_block_height,
	initialize_network, verify_node_bitswap, verify_parachain_binaries,
	verify_warp_sync_completed, wait_for_block_height, wait_for_finalized_height,
	wait_for_fullnode, wait_for_new_block_beyond, wait_for_relay_chain_to_sync,
	wait_for_session_change_on_node, ParachainSnapshots, BLOCK_PRODUCTION_TIMEOUT_SECS,
	NETWORK_READY_TIMEOUT_SECS, NODE_LOG_CONFIG, PARACHAIN_TEST_DATA_PATTERN, PARA_ID, PARACHAIN_BINARY, SYNC_TIMEOUT_SECS,
	TEST_DATA_SIZE,
};
use crate::test_log;
use anyhow::{Context, Result};
use env_logger::Env;
use futures::try_join;
use zombienet_orchestrator::AddCollatorOptions;

const MIN_BLOCKS_BEFORE_SYNC_NODE: u64 = 10;
const SESSION_CHANGE_TIMEOUT_SECS: u64 = 300;
const WARP_PRUNING_BLOCKS: u32 = 100;

fn snapshot_store_data(store_index: usize) -> (Vec<u8>, String, String) {
	let block = (store_index as u64 + 1) * 10;
	let pattern = format!("PARA_GENDB_{:04}_", block);
	let data = generate_test_data(TEST_DATA_SIZE, pattern.as_bytes());
	let (hash, cid) = content_hash_and_cid(&data);
	(data, hash, cid)
}

struct ResolvedSnapshots {
	collator: String,
	relay: String,
	chain_spec: String,
	relay_chain_spec: String,
}

fn load_snapshot_paths() -> Result<ResolvedSnapshots> {
	let snapshot_dir = "tests/zombie_ci/storage_chain/fixtures/test-databases";
	let snapshot_base = std::path::Path::new(&snapshot_dir);

	let collator = std::fs::canonicalize(snapshot_base.join("pruned.tgz"))
		.with_context(|| format!("Collator snapshot not found in {}", snapshot_dir))?;
	let relay = std::fs::canonicalize(snapshot_base.join("relay.tgz"))
		.with_context(|| format!("Relay snapshot not found in {}", snapshot_dir))?;
	let chain_spec = std::fs::canonicalize(snapshot_base.join("raw-chain-spec.json"))
		.with_context(|| format!("Raw chain spec not found in {}", snapshot_dir))?;
	let relay_chain_spec =
		std::fs::canonicalize(snapshot_base.join("raw-relay-chain-spec.json"))
			.with_context(|| format!("Raw relay chain spec not found in {}", snapshot_dir))?;

	let resolved = ResolvedSnapshots {
		collator: collator.to_string_lossy().to_string(),
		relay: relay.to_string_lossy().to_string(),
		chain_spec: chain_spec.to_string_lossy().to_string(),
		relay_chain_spec: relay_chain_spec.to_string_lossy().to_string(),
	};

	log::info!("Collator DB snapshot: {}", resolved.collator);
	log::info!("Relay DB snapshot: {}", resolved.relay);
	log::info!("Raw chain spec: {}", resolved.chain_spec);
	log::info!("Raw relay chain spec: {}", resolved.relay_chain_spec);

	Ok(resolved)
}

impl ResolvedSnapshots {
	fn as_parachain_snapshots(&self) -> ParachainSnapshots<'_> {
		ParachainSnapshots {
			collator: &self.collator,
			relay: &self.relay,
			chain_spec: &self.chain_spec,
			relay_chain_spec: &self.relay_chain_spec,
		}
	}
}

fn get_para_node_args() -> Vec<String> {
	vec!["--ipfs-server".into(), NODE_LOG_CONFIG.into()]
}

#[tokio::test(flavor = "multi_thread")]
async fn parachain_warp_sync_with_pruning_test() -> Result<()> {
	const TEST: &str = "para_warp_sync_pruning";
	let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info")).try_init();

	test_log!(TEST, "=== Parachain Warp Sync + Storage Chain Test (using DB snapshot) ===");
	log::info!("This test requires 3 relay validators for GRANDPA finality");
	log::info!(
		"Sync-node will use --sync=warp --blocks-pruning={}",
		WARP_PRUNING_BLOCKS,
	);

	verify_parachain_binaries()?;

	let snaps = load_snapshot_paths()?;

	let para_args = get_para_node_args();
	let config = build_parachain_network_config_three_relay_validators_with_snapshots(
		para_args,
		Some(snaps.as_parachain_snapshots()),
	)?;
	let mut network = initialize_network(config).await?;
	network.wait_until_is_up(NETWORK_READY_TIMEOUT_SECS).await?;

	let relay_alice = network.get_node("alice").context("Failed to get relay alice node")?;
	log::info!("Waiting for relay chain session change...");
	wait_for_session_change_on_node(relay_alice, SESSION_CHANGE_TIMEOUT_SECS)
		.await
		.context("Failed to detect session change on relay chain")?;

	let collator1 = network.get_node("collator-1").context("Failed to get collator-1 node")?;

	let snapshot_height = get_best_block_height(collator1).await?;
	log::info!("Snapshot best block height: {}", snapshot_height);
	wait_for_new_block_beyond(collator1, snapshot_height, BLOCK_PRODUCTION_TIMEOUT_SECS).await?;
	log::info!("Collator is producing new blocks on top of the snapshot");

	let test_data = generate_test_data(TEST_DATA_SIZE, PARACHAIN_TEST_DATA_PATTERN);
	let (content_hash, cid) = content_hash_and_cid(&test_data);
	log::info!(
		"Storing {} bytes of fresh test data (hash: {}, CID: {})",
		test_data.len(),
		content_hash,
		cid,
	);

	let nonce = get_alice_nonce(collator1).await?;
	let (store_block, _) = authorize_and_store_data(collator1, &test_data, nonce).await?;
	log::info!("Store completed at block {}", store_block);

	verify_node_bitswap(collator1, &test_data, 30, "Collator-1").await?;

	let target_block = std::cmp::max(store_block, MIN_BLOCKS_BEFORE_SYNC_NODE);
	log::info!("Waiting for block {} and finality", target_block);
	try_join!(
		wait_for_block_height(collator1, target_block, BLOCK_PRODUCTION_TIMEOUT_SECS),
		wait_for_finalized_height(collator1, target_block, BLOCK_PRODUCTION_TIMEOUT_SECS),
	)?;

	log::info!("Adding sync-node with --sync=warp --blocks-pruning");
	let para_binary = PARACHAIN_BINARY;
	let sync_node_opts = AddCollatorOptions {
		command: Some(para_binary.try_into()?),
		args: vec![
			"--sync=warp".into(),
			"--ipfs-server".into(),
			format!("--blocks-pruning={}", WARP_PRUNING_BLOCKS).as_str().into(),
			format!("{},db=debug", NODE_LOG_CONFIG).as_str().into(),
		],
		is_validator: false,
		..Default::default()
	};

	network.add_collator("sync-node", sync_node_opts, PARA_ID).await?;
	let sync_node = network.get_node("sync-node").context("Failed to get sync-node")?;

	wait_for_fullnode(sync_node).await?;

	log::info!("Waiting for sync-node's embedded relay chain to sync...");
	wait_for_relay_chain_to_sync(sync_node, SYNC_TIMEOUT_SECS)
		.await
		.context("Sync node's embedded relay chain did not sync")?;

	log::info!("Verifying sync-node's progress (target: block {})", target_block);
	wait_for_block_height(sync_node, target_block, SYNC_TIMEOUT_SECS)
		.await
		.context("Sync node failed to sync via warp sync")?;

	verify_warp_sync_completed(sync_node).await?;

	log::info!("Verifying sync-node serves snapshot data via bitswap");
	for i in 0..3 {
		let (data, _, cid) = snapshot_store_data(i);
		log::info!("Checking snapshot CID {} on sync-node", cid);
		verify_node_bitswap(sync_node, &data, 30, &format!("sync-node (snapshot #{})", i + 1))
			.await?;
	}
	log::info!("✓ sync-node serves snapshot data after --sync=warp");

	verify_node_bitswap(sync_node, &test_data, 30, "sync-node").await?;
	log::info!("✓ Bitswap works from sync-node for freshly stored data");

	test_log!(
		TEST,
		"=== Parachain Warp Sync + Storage Chain Test (with pruning) PASSED ===",
	);
	network.destroy().await?;
	Ok(())
}
