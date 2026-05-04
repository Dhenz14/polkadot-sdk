// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

use super::utils::{
	authorize_and_store_data, build_parachain_network_config_three_relay_validators,
	content_hash_and_cid, generate_test_data, get_alice_nonce, get_best_block_height,
	initialize_network, verify_node_bitswap, verify_parachain_binaries,
	verify_warp_sync_completed, wait_for_block_height, wait_for_finalized_height,
	wait_for_fullnode, wait_for_new_block_beyond, wait_for_relay_chain_to_sync,
	wait_for_session_change_on_node, BLOCK_PRODUCTION_TIMEOUT_SECS,
	NETWORK_READY_TIMEOUT_SECS, NODE_LOG_CONFIG, PARACHAIN_TEST_DATA_PATTERN, PARA_ID,
	PARACHAIN_BINARY, SYNC_TIMEOUT_SECS, TEST_DATA_SIZE,
};
use crate::test_log;
use anyhow::{Context, Result};
use env_logger::Env;
use futures::try_join;
use zombienet_orchestrator::AddCollatorOptions;

const SESSION_CHANGE_TIMEOUT_SECS: u64 = 300;
const WARP_PRUNING_BLOCKS: u32 = 100;

fn get_para_node_args() -> Vec<String> {
	vec!["--ipfs-server".into(), NODE_LOG_CONFIG.into()]
}

#[tokio::test(flavor = "multi_thread")]
async fn parachain_tip_rollover_test() -> Result<()> {
	const TEST: &str = "para_tip_rollover";
	let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info")).try_init();

	test_log!(TEST, "=== Parachain Tip Rollover Test (StorageChainBlockImport) ===");
	log::info!("3 relay validators + 1 collator + 1 warp-sync-node, no DB snapshots");

	verify_parachain_binaries()?;

	let para_args = get_para_node_args();
	let config = build_parachain_network_config_three_relay_validators(para_args)?;
	let mut network = initialize_network(config).await?;
	network.wait_until_is_up(NETWORK_READY_TIMEOUT_SECS).await?;

	let relay_alice = network.get_node("alice").context("Failed to get relay alice node")?;
	log::info!("Waiting for relay chain session change...");
	wait_for_session_change_on_node(relay_alice, SESSION_CHANGE_TIMEOUT_SECS)
		.await
		.context("Failed to detect session change on relay chain")?;

	let collator1 = network.get_node("collator-1").context("Failed to get collator-1 node")?;

	let baseline_height = get_best_block_height(collator1).await?;
	log::info!("Collator baseline height: {}", baseline_height);
	wait_for_new_block_beyond(collator1, baseline_height, BLOCK_PRODUCTION_TIMEOUT_SECS).await?;
	log::info!("Collator is producing fresh blocks");

	log::info!("Adding sync-node with --sync=warp --blocks-pruning before any test data is stored");
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

	let warp_target = get_best_block_height(collator1).await?;
	log::info!("Waiting for sync-node to reach block {} via warp", warp_target);
	wait_for_block_height(sync_node, warp_target, SYNC_TIMEOUT_SECS)
		.await
		.context("Sync node failed to sync via warp sync")?;
	verify_warp_sync_completed(sync_node).await?;
	log::info!("✓ sync-node finished warp sync");

	log::info!("Storing fresh test data on collator AFTER sync-node has caught up");
	let test_data = generate_test_data(TEST_DATA_SIZE, PARACHAIN_TEST_DATA_PATTERN);
	let (content_hash, cid) = content_hash_and_cid(&test_data);
	log::info!(
		"Fresh data: {} bytes (hash: {}, CID: {})",
		test_data.len(),
		content_hash,
		cid,
	);

	let nonce = get_alice_nonce(collator1).await?;
	let (store_block, _) = authorize_and_store_data(collator1, &test_data, nonce).await?;
	log::info!("Store completed at collator block {}", store_block);

	verify_node_bitswap(collator1, &test_data, 30, "collator-1 (post-store)").await?;

	log::info!("Waiting for collator and sync-node to reach block {} (finalized)", store_block);
	try_join!(
		wait_for_finalized_height(collator1, store_block, BLOCK_PRODUCTION_TIMEOUT_SECS),
		wait_for_block_height(sync_node, store_block, SYNC_TIMEOUT_SECS),
	)?;

	log::info!(
		"Verifying sync-node serves freshly-stored test_data via bitswap \
		 (proves StorageChainBlockImport populated TRANSACTION column)"
	);
	verify_node_bitswap(sync_node, &test_data, 30, "sync-node (post-warp tip)").await?;
	log::info!("✓ sync-node serves data stored AFTER its warp sync completed");

	test_log!(TEST, "=== Parachain Tip Rollover Test PASSED ===");
	network.destroy().await?;
	Ok(())
}
