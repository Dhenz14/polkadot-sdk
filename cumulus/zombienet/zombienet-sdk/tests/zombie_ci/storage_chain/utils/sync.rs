// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

use super::config::*;
use anyhow::{anyhow, Context, Result};
use std::time::Duration;
use zombienet_orchestrator::network::node::LogLineCountOptions;
use zombienet_sdk::subxt::{config::substrate::SubstrateConfig, OnlineClient};

pub fn log_line_at_least_once(timeout_secs: u64) -> LogLineCountOptions {
	LogLineCountOptions::new(|count| count >= 1, Duration::from_secs(timeout_secs), false)
}

pub fn log_line_absent(timeout_secs: u64) -> LogLineCountOptions {
	LogLineCountOptions::no_occurences_within_timeout(Duration::from_secs(timeout_secs))
}

pub async fn expect_log_line(
	node: &zombienet_sdk::NetworkNode,
	pattern: &str,
	timeout_secs: u64,
	error_msg: &str,
) -> Result<()> {
	let result = node
		.wait_log_line_count_with_timeout(pattern, false, log_line_at_least_once(timeout_secs))
		.await
		.context(format!("Failed to check log: {}", pattern))?;
	if !result.success() {
		anyhow::bail!("{}", error_msg);
	}
	Ok(())
}

pub async fn expect_no_log_line(
	node: &zombienet_sdk::NetworkNode,
	pattern: &str,
	timeout_secs: u64,
	error_msg: &str,
) -> Result<()> {
	let result = node
		.wait_log_line_count_with_timeout(pattern, false, log_line_absent(timeout_secs))
		.await
		.context(format!("Failed to check absence of log: {}", pattern))?;
	if !result.success() {
		anyhow::bail!("{}", error_msg);
	}
	Ok(())
}

pub async fn verify_warp_sync_completed(node: &zombienet_sdk::NetworkNode) -> Result<()> {
	log::info!("Verifying warp sync completed");
	expect_log_line(
		node,
		"Warp sync is complete",
		LOG_TIMEOUT_SECS,
		"Node did not complete warp sync",
	)
	.await?;
	wait_for_node_idle(node, SYNC_TIMEOUT_SECS)
		.await
		.context("Node did not reach idle state after warp sync")?;
	expect_no_log_line(
		node,
		"verification failed",
		LOG_ERROR_TIMEOUT_SECS,
		"Node logged verification errors",
	)
	.await?;
	log::info!("✓ Warp sync completed and node is idle");
	Ok(())
}

pub async fn wait_for_fullnode(node: &zombienet_sdk::NetworkNode) -> Result<()> {
	node.wait_metric_with_timeout(
		NODE_ROLE_METRIC,
		|role| role == FULLNODE_ROLE_VALUE,
		METRIC_TIMEOUT_SECS,
	)
	.await
	.context("Node did not become full node")
}

pub async fn wait_for_block_height(
	node: &zombienet_sdk::NetworkNode,
	min_height: u64,
	timeout_secs: u64,
) -> Result<()> {
	node.wait_metric_with_timeout(
		BEST_BLOCK_METRIC,
		|height| height >= min_height as f64,
		timeout_secs,
	)
	.await
	.context(format!("Node did not reach block height {}", min_height))
}

pub async fn get_best_block_height(node: &zombienet_sdk::NetworkNode) -> Result<u64> {
	let height = node
		.reports(BEST_BLOCK_METRIC)
		.await
		.context("Failed to read best block metric")?;
	Ok(height as u64)
}

pub async fn wait_for_new_block_beyond(
	node: &zombienet_sdk::NetworkNode,
	baseline_height: u64,
	timeout_secs: u64,
) -> Result<()> {
	let target = baseline_height + 1;
	log::info!(
		"Waiting for new block production beyond snapshot height {} (target: >= {})",
		baseline_height,
		target
	);
	wait_for_block_height(node, target, timeout_secs).await
}

pub async fn wait_for_finalized_height(
	node: &zombienet_sdk::NetworkNode,
	min_height: u64,
	timeout_secs: u64,
) -> Result<()> {
	node.wait_metric_with_timeout(
		FINALIZED_BLOCK_METRIC,
		|height| height >= min_height as f64,
		timeout_secs,
	)
	.await
	.context(format!("Node did not finalize block height {}", min_height))
}

pub async fn wait_for_node_idle(
	node: &zombienet_sdk::NetworkNode,
	timeout_secs: u64,
) -> Result<()> {
	node.wait_metric_with_timeout(
		IS_MAJOR_SYNCING_METRIC,
		|value| value == IDLE_VALUE,
		timeout_secs,
	)
	.await
	.context("Node did not reach idle state (still syncing)")
}

pub async fn wait_for_relay_chain_to_sync(
	node: &zombienet_sdk::NetworkNode,
	timeout_secs: u64,
) -> Result<()> {
	let result = node
		.wait_log_line_count_with_timeout(
			r"Update at relay chain block.*included: #[1-9]",
			false,
			log_line_at_least_once(timeout_secs),
		)
		.await
		.context("Failed to check relay chain sync status")?;

	if !result.success() {
		anyhow::bail!(
			"Embedded relay chain did not sync - no 'included' parachain blocks seen within {}s",
			timeout_secs
		);
	}

	log::info!("✓ Embedded relay chain is synced (seeing included parachain blocks)");
	Ok(())
}

const WAIT_MAX_BLOCKS_FOR_SESSION: u32 = 50;

async fn is_session_change_block(
	block: &zombienet_sdk::subxt::blocks::Block<
		SubstrateConfig,
		zombienet_sdk::subxt::OnlineClient<SubstrateConfig>,
	>,
) -> Result<bool> {
	let events = block.events().await.context("Failed to fetch block events")?;
	Ok(events.iter().any(|event| {
		event
			.as_ref()
			.is_ok_and(|e| e.pallet_name() == "Session" && e.variant_name() == "NewSession")
	}))
}

pub async fn wait_for_first_session_change(
	relay_client: &OnlineClient<SubstrateConfig>,
	timeout_secs: u64,
) -> Result<()> {
	wait_for_nth_session_change(relay_client, 1, timeout_secs).await
}

pub async fn wait_for_nth_session_change(
	relay_client: &OnlineClient<SubstrateConfig>,
	mut sessions_to_wait: u32,
	timeout_secs: u64,
) -> Result<()> {
	log::info!("Waiting for {} session change(s) on relay chain...", sessions_to_wait);

	let wait_future = async {
		let mut blocks_sub = relay_client
			.blocks()
			.subscribe_finalized()
			.await
			.context("Failed to subscribe to finalized blocks")?;

		let mut waited_block_count = 0u32;

		while let Some(block_result) = blocks_sub.next().await {
			let block = block_result.context("Error receiving block")?;
			log::debug!("Relay chain finalized block #{}", block.number());

			if is_session_change_block(&block).await? {
				sessions_to_wait -= 1;
				log::info!(
					"Session change detected at relay block #{}. {} more to wait.",
					block.number(),
					sessions_to_wait
				);

				if sessions_to_wait == 0 {
					log::info!("All required session changes detected");
					return Ok(());
				}

				waited_block_count = 0;
			} else {
				waited_block_count += 1;
				if waited_block_count >= WAIT_MAX_BLOCKS_FOR_SESSION {
					anyhow::bail!(
						"Waited {} blocks without session change. Session should have arrived by now.",
						WAIT_MAX_BLOCKS_FOR_SESSION
					);
				}
			}
		}

		anyhow::bail!("Block subscription ended unexpectedly")
	};

	tokio::time::timeout(Duration::from_secs(timeout_secs), wait_future)
		.await
		.map_err(|_| anyhow!("Timeout waiting for session change after {}s", timeout_secs))?
}

pub async fn wait_for_session_change_on_node(
	relay_node: &zombienet_sdk::NetworkNode,
	timeout_secs: u64,
) -> Result<()> {
	let relay_client: OnlineClient<SubstrateConfig> =
		relay_node.wait_client().await.context("Failed to get relay chain client")?;
	wait_for_first_session_change(&relay_client, timeout_secs).await
}
