// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Database generation test for parachain sync tests.
//!
//! This test generates two pre-built databases that future sync tests can use
//! as starting points, avoiding the need to produce blocks from genesis every time.
//!
//! ## What it produces
//!
//! - **Archive DB**: A collator node with no pruning, containing `TARGET_BLOCKS` blocks with
//!   indexed transaction data stored every `STORE_INTERVAL` blocks.
//! - **Pruned DB**: A full-sync node with short `--blocks-pruning`, synced from the archive
//!   collator. Old blocks beyond the pruning window are deleted.
//! - **Manifest**: JSON metadata describing renewable entries for tests that consume snapshots.
//!
//! ## How it works
//!
//! 1. Spawns a parachain network: 2 relay validators + 1 archive collator + 1 pruned full node
//! 2. Waits for session change so the parachain starts producing blocks
//! 3. Authorizes Alice/Bob for store transactions upfront
//! 4. Every `STORE_INTERVAL` blocks, stores 2KB of unique test data via the collator
//! 5. Waits for both nodes to reach block `TARGET_BLOCKS` (finalized)
//! 6. Copies parachain databases and the renewable-entry manifest to `$DB_OUTPUT_DIR` (default:
//!    `./zombienet/test-databases/`)
//!
//! ## Environment Variables
//!
//! - `TARGET_BLOCKS` (default: `1000`)
//! - `STORE_INTERVAL` (default: `10`)
//! - `PRUNING_BLOCKS` (default: fixture retention period, `200`)
//! - `RENEWABLE_STORE_COUNT` (default: `10`)
//! - `RENEWAL_PASS_BLOCK` (default: `105`)
//! - `RENEWAL_PASS_INTERVAL` (default: `80`)
//! - `AUTHORIZE_TRANSACTIONS` (default: `100`)
//! - `DB_OUTPUT_DIR`: Output directory for generated databases (default:
//!   `./zombienet/test-databases/`)
//! - Standard parachain env vars (see `parachain_sync_storage` module docs)
//!
//! ## Running
//!
//! ```bash
//! just para-gen-db
//! ```

use super::utils::{
	archive_manifest_path, blake2_256, get_alice_nonce, hash_to_cid, initialize_network,
	renew_data_with_hash, test_data_for_store_target_block, verify_parachain_binaries,
	wait_for_block_height, wait_for_finalized_height, wait_for_session_change_on_node,
	RenewableEntryManifest, SnapshotManifest, BLOCK_PRODUCTION_TIMEOUT_SECS,
	FIXTURE_RETENTION_PERIOD, NETWORK_READY_TIMEOUT_SECS, NODE_LOG_CONFIG, PARACHAIN_BINARY,
	PARACHAIN_CHAIN_SPEC, PARA_ID, RELAY_BINARY, RELAY_CHAIN, SYNC_TIMEOUT_SECS, TEST_DATA_SIZE,
};
use crate::test_log;
use anyhow::{anyhow, Context, Result};
use env_logger::Env;
use flate2::{write::GzEncoder, Compression};
use std::path::{Path, PathBuf};
use zombienet_sdk::{
	subxt::{
		config::substrate::{SubstrateConfig, SubstrateExtrinsicParamsBuilder},
		dynamic::{tx, Value},
		ext::scale_value::value,
		OnlineClient,
	},
	subxt_signer::sr25519::dev,
	NetworkConfig, NetworkConfigBuilder,
};

const SESSION_CHANGE_TIMEOUT_SECS: u64 = 300;
const DB_OUTPUT_DIR_ENV: &str = "DB_OUTPUT_DIR";
const DEFAULT_DB_OUTPUT_DIR: &str = "./zombienet/test-databases";

/// Timeout for reaching the configured target block count. Parachain blocks are ~12s each in local
/// testnet, so 500 blocks ≈ 6000s. Add generous margin.
const GEN_TIMEOUT_SECS: u64 = 10000;

struct GenDbConfig {
	target_blocks: u64,
	store_interval: u64,
	pruning_blocks: u32,
	renewable_store_count: u64,
	renewal_pass_block: u64,
	renewal_pass_interval: u64,
	last_renewal_pass_ceiling: u64,
	authorize_transactions: u32,
	authorize_bytes: u64,
}

impl GenDbConfig {
	fn from_env() -> Result<Self> {
		fn parse_env<T: std::str::FromStr>(name: &str, default: T) -> Result<T>
		where
			T::Err: std::fmt::Display,
		{
			match std::env::var(name) {
				Ok(v) => v.parse::<T>().map_err(|e| anyhow!("{name}: {e}")),
				Err(_) => Ok(default),
			}
		}

		let target_blocks = parse_env("TARGET_BLOCKS", 1000u64)?;
		let store_interval = parse_env("STORE_INTERVAL", 10u64)?;
		let pruning_blocks = parse_env("PRUNING_BLOCKS", FIXTURE_RETENTION_PERIOD)?;
		let renewable_store_count = parse_env("RENEWABLE_STORE_COUNT", 10u64)?;
		let renewal_pass_block = parse_env("RENEWAL_PASS_BLOCK", 105u64)?;
		let renewal_pass_interval = parse_env("RENEWAL_PASS_INTERVAL", 80u64)?;
		let last_renewal_pass_ceiling = target_blocks.saturating_sub(30);

		let authorize_transactions = match std::env::var("AUTHORIZE_TRANSACTIONS") {
			Ok(v) => v.parse::<u32>().map_err(|e| anyhow!("AUTHORIZE_TRANSACTIONS: {e}"))?,
			Err(_) => {
				if std::env::var("TARGET_BLOCKS").is_ok() || std::env::var("STORE_INTERVAL").is_ok()
				{
					let store_count = (target_blocks / store_interval).max(1);
					((2 * store_count) + 10).min(200) as u32
				} else {
					100
				}
			},
		};
		let authorize_bytes = (authorize_transactions as u64) * (TEST_DATA_SIZE as u64) * 2;

		Ok(Self {
			target_blocks,
			store_interval,
			pruning_blocks,
			renewable_store_count,
			renewal_pass_block,
			renewal_pass_interval,
			last_renewal_pass_ceiling,
			authorize_transactions,
			authorize_bytes,
		})
	}
}

fn get_db_output_dir() -> PathBuf {
	let dir = std::env::var(DB_OUTPUT_DIR_ENV).unwrap_or_else(|_| DEFAULT_DB_OUTPUT_DIR.into());
	PathBuf::from(dir)
}

fn build_gendb_network_config(pruning_blocks: u32) -> Result<NetworkConfig> {
	let relay_binary = RELAY_BINARY.to_string();
	let para_binary = PARACHAIN_BINARY.to_string();
	let para_chain_spec = PARACHAIN_CHAIN_SPEC.to_string();
	let relay_chain = RELAY_CHAIN.to_string();
	let para_id = PARA_ID;

	let relay_args: Vec<_> = vec!["-lruntime=debug"].into_iter().map(|s| s.into()).collect();
	let relay_args2 = relay_args.clone();

	let collator_args: Vec<_> =
		vec!["--ipfs-server", NODE_LOG_CONFIG].into_iter().map(|s| s.into()).collect();

	let pruning_flag = format!("--blocks-pruning={}", pruning_blocks);
	let pruned_args: Vec<_> =
		vec!["--sync=full", "--ipfs-server", pruning_flag.as_str(), NODE_LOG_CONFIG]
			.into_iter()
			.map(|s| s.into())
			.collect();

	NetworkConfigBuilder::new()
		.with_relaychain(|relaychain| {
			relaychain
				.with_chain(relay_chain.as_str())
				.with_default_command(relay_binary.as_str())
				.with_validator(|node| {
					node.with_name("alice").validator(true).with_args(relay_args)
				})
				.with_validator(|node| node.with_name("bob").validator(true).with_args(relay_args2))
		})
		.with_parachain(|parachain| {
			parachain
				.with_id(para_id)
				.with_chain_spec_path(para_chain_spec.as_str())
				.cumulus_based(true)
				.with_collator(|c| {
					c.with_name("collator-1")
						.validator(true)
						.with_command(para_binary.as_str())
						.with_args(collator_args)
				})
				.with_collator(|c| {
					c.with_name("pruned-node")
						.validator(false)
						.with_command(para_binary.as_str())
						.with_args(pruned_args)
				})
		})
		.with_global_settings(|gs| match std::env::var("ZOMBIENET_SDK_BASE_DIR") {
			Ok(val) => gs.with_base_dir(val),
			_ => gs,
		})
		.build()
		.map_err(|errs| {
			let message = errs.into_iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ");
			anyhow!("config errs: {message}")
		})
}

/// Authorize Alice for a large batch of store transactions upfront.
async fn authorize_bulk_storage(
	client: &OnlineClient<SubstrateConfig>,
	nonce: u64,
	authorize_transactions: u32,
	authorize_bytes: u64,
) -> Result<()> {
	let signer = dev::alice();

	let authorize_call = zombienet_sdk::subxt::tx::dynamic(
		"Sudo",
		"sudo",
		vec![value! {
			TransactionStorage(authorize_account {
				who: Value::from_bytes(signer.public_key().0),
				transactions: authorize_transactions,
				bytes: authorize_bytes
			})
		}],
	);

	let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
	let timeout = std::time::Duration::from_secs(60);

	tokio::time::timeout(timeout, async {
		let progress =
			client.tx().sign_and_submit_then_watch(&authorize_call, &signer, params).await?;
		super::utils::tx::wait_for_in_best_block(progress).await?;
		Ok::<_, anyhow::Error>(())
	})
	.await
	.map_err(|_| anyhow!("bulk authorization timed out"))??;

	log::info!(
		"Authorized Alice for {} transactions / {} bytes",
		authorize_transactions,
		authorize_bytes,
	);
	Ok(())
}

async fn authorize_bob_for_renewals(
	client: &OnlineClient<SubstrateConfig>,
	nonce: u64,
	authorize_transactions: u32,
	authorize_bytes: u64,
) -> Result<()> {
	let signer = dev::alice();
	let bob = dev::bob();

	let authorize_call = zombienet_sdk::subxt::tx::dynamic(
		"Sudo",
		"sudo",
		vec![value! {
			TransactionStorage(authorize_account {
				who: Value::from_bytes(bob.public_key().0),
				transactions: authorize_transactions,
				bytes: authorize_bytes
			})
		}],
	);

	let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
	let timeout = std::time::Duration::from_secs(60);

	tokio::time::timeout(timeout, async {
		let progress =
			client.tx().sign_and_submit_then_watch(&authorize_call, &signer, params).await?;
		super::utils::tx::wait_for_in_best_block(progress).await?;
		Ok::<_, anyhow::Error>(())
	})
	.await
	.map_err(|_| anyhow!("bob authorization timed out"))??;

	log::info!(
		"Authorized Bob for {} transactions / {} bytes",
		authorize_transactions,
		authorize_bytes
	);
	Ok(())
}

async fn store_data(
	client: &OnlineClient<SubstrateConfig>,
	data: &[u8],
	nonce: u64,
) -> Result<u64> {
	let signer = dev::alice();

	let store_call = tx("TransactionStorage", "store", vec![Value::from_bytes(data)]);
	let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
	let timeout = std::time::Duration::from_secs(60);

	let (block_hash, _events) = tokio::time::timeout(timeout, async {
		let progress = client.tx().sign_and_submit_then_watch(&store_call, &signer, params).await?;
		super::utils::tx::wait_for_in_best_block(progress).await
	})
	.await
	.map_err(|_| anyhow!("store transaction timed out (nonce={})", nonce))??;

	let block = client.blocks().at(block_hash).await?;
	Ok(block.number() as u64)
}

async fn store_data_finalized(
	client: &OnlineClient<SubstrateConfig>,
	data: &[u8],
	nonce: u64,
) -> Result<u64> {
	let signer = dev::alice();

	let store_call = tx("TransactionStorage", "store", vec![Value::from_bytes(data)]);
	let params = SubstrateExtrinsicParamsBuilder::new().nonce(nonce).build();
	let timeout = std::time::Duration::from_secs(120);

	let (block_hash, _events) = tokio::time::timeout(timeout, async {
		let progress = client.tx().sign_and_submit_then_watch(&store_call, &signer, params).await?;
		super::utils::tx::wait_for_finalized(progress).await
	})
	.await
	.map_err(|_| anyhow!("store_finalized transaction timed out (nonce={})", nonce))??;

	let block = client.blocks().at(block_hash).await?;
	Ok(block.number() as u64)
}

/// Directories to exclude from snapshots. Zombienet injects its own keystore and
/// network identity for each node, so including the original node's keys would
/// conflict and potentially prevent the collator from producing blocks.
const SNAPSHOT_EXCLUDE_DIRS: &[&str] = &["keystore", "network"];

/// Create a `.tgz` archive of a node's directories, suitable for zombienet's
/// `with_db_snapshot()`.
///
/// Zombienet's native provider extracts the tarball into `<namespace>/<node_name>/`,
/// so paths inside the archive must be relative to the node's base dir.
///
/// `node_base_dir` is `<zombienet_base>/<node_name>/`. By default, archives `data/`.
/// Pass additional directory names (e.g. `&["relay-data"]`) to include them too —
/// needed for collator snapshots that must contain the embedded relay chain DB.
///
/// Excludes `keystore/` and `network/` subdirectories which zombienet manages itself.
fn create_db_snapshot_tgz(
	node_base_dir: &Path,
	output_path: &Path,
	extra_dirs: &[&str],
) -> Result<u64> {
	let data_dir = node_base_dir.join("data");
	anyhow::ensure!(data_dir.exists(), "data dir does not exist: {}", data_dir.display());

	if let Some(parent) = output_path.parent() {
		std::fs::create_dir_all(parent)?;
	}

	let file = std::fs::File::create(output_path)
		.with_context(|| format!("Failed to create {}", output_path.display()))?;
	let enc = GzEncoder::new(file, Compression::fast());
	let mut tar = tar::Builder::new(enc);
	tar.mode(tar::HeaderMode::Deterministic);

	// Walk a directory recursively, skipping keystore/ and network/ dirs.
	fn add_dir_filtered(
		tar: &mut tar::Builder<GzEncoder<std::fs::File>>,
		src_dir: &Path,
		archive_prefix: &Path,
	) -> Result<()> {
		tar.append_dir(archive_prefix, src_dir)
			.with_context(|| format!("Failed to add dir {}", src_dir.display()))?;

		for entry in std::fs::read_dir(src_dir)
			.with_context(|| format!("Failed to read dir {}", src_dir.display()))?
		{
			let entry = entry?;
			let file_name = entry.file_name();
			let file_name_str = file_name.to_string_lossy();
			let src_path = entry.path();
			let archive_path = archive_prefix.join(&file_name);

			if entry.file_type()?.is_dir() {
				if SNAPSHOT_EXCLUDE_DIRS.iter().any(|d| *d == file_name_str) {
					log::info!("Snapshot: skipping {}", src_path.display());
					continue;
				}
				add_dir_filtered(tar, &src_path, &archive_path)?;
			} else {
				tar.append_path_with_name(&src_path, &archive_path)
					.with_context(|| format!("Failed to add {}", src_path.display()))?;
			}
		}
		Ok(())
	}

	// Always include data/
	add_dir_filtered(&mut tar, &data_dir, Path::new("data"))?;

	// Include any extra directories (e.g. relay-data/ for collator snapshots)
	for dir_name in extra_dirs {
		let extra_dir = node_base_dir.join(dir_name);
		if extra_dir.exists() {
			log::info!("Snapshot: including extra dir {}/", dir_name);
			add_dir_filtered(&mut tar, &extra_dir, Path::new(dir_name))?;
		} else {
			log::warn!("Snapshot: extra dir {} does not exist, skipping", extra_dir.display());
		}
	}

	tar.finish()?;
	drop(tar);

	let size = std::fs::metadata(output_path)?.len();
	Ok(size)
}

#[tokio::test(flavor = "multi_thread")]
async fn parachain_generate_databases() -> Result<()> {
	const TEST: &str = "para_gen_db";
	let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info")).try_init();
	let cfg = GenDbConfig::from_env()?;

	test_log!(TEST, "=== Parachain Database Generation ===");
	log::info!(
		"GenDbConfig: target_blocks={}, store_interval={}, fixture_retention_period={}, pruning_blocks={}, renewable_store_count={}, renewal_pass_block={}, renewal_pass_interval={}, authorize_transactions={}, authorize_bytes={}",
		cfg.target_blocks,
		cfg.store_interval,
		FIXTURE_RETENTION_PERIOD,
		cfg.pruning_blocks,
		cfg.renewable_store_count,
		cfg.renewal_pass_block,
		cfg.renewal_pass_interval,
		cfg.authorize_transactions,
		cfg.authorize_bytes,
	);

	verify_parachain_binaries()?;

	let output_dir = get_db_output_dir();
	std::fs::create_dir_all(&output_dir)
		.with_context(|| format!("Failed to create output dir {}", output_dir.display()))?;
	log::info!("Output directory: {}", output_dir.display());

	// === Phase 1: Spawn network (archive collator + pruned full node) ===
	test_log!(
		TEST,
		"Phase 1: Spawning network (collator + pruned-node with --blocks-pruning={})",
		cfg.pruning_blocks
	);

	let config = build_gendb_network_config(cfg.pruning_blocks)?;
	let network = initialize_network(config).await?;
	network.wait_until_is_up(NETWORK_READY_TIMEOUT_SECS).await?;

	let relay_alice = network.get_node("alice").context("Failed to get relay alice node")?;

	log::info!("Waiting for relay chain session change...");
	wait_for_session_change_on_node(relay_alice, SESSION_CHANGE_TIMEOUT_SECS)
		.await
		.context("Failed to detect session change on relay chain")?;

	// === Phase 2: Authorize and store data periodically ===
	test_log!(
		TEST,
		"Phase 2: Storing data every {} blocks up to block {}",
		cfg.store_interval,
		cfg.target_blocks
	);

	let collator1 = network.get_node("collator-1").context("Failed to get collator-1 node")?;
	let collator_client: OnlineClient<SubstrateConfig> = collator1.wait_client().await?;

	let mut nonce = get_alice_nonce(collator1).await?;

	authorize_bulk_storage(
		&collator_client,
		nonce,
		cfg.authorize_transactions,
		cfg.authorize_bytes,
	)
	.await?;
	nonce += 1;

	authorize_bob_for_renewals(
		&collator_client,
		nonce,
		cfg.authorize_transactions,
		cfg.authorize_bytes,
	)
	.await?;
	nonce += 1;

	// Wait for parachain to start producing and track current height
	wait_for_block_height(collator1, 1, BLOCK_PRODUCTION_TIMEOUT_SECS).await?;

	let mut renewable_entries: Vec<RenewableEntryManifest> = Vec::new();
	let mut next_renewal_block: u64 = cfg.renewal_pass_block;
	let mut bob_nonce_counter: u64 = 0;
	let mut next_store_block: u64 = cfg.store_interval;
	let mut store_count: u64 = 0;

	while next_store_block <= cfg.target_blocks {
		let renewal_ready = !renewable_entries.is_empty() ||
			(store_count >= cfg.renewable_store_count &&
				next_store_block > cfg.renewable_store_count * cfg.store_interval);
		if renewal_ready &&
			next_renewal_block <= cfg.last_renewal_pass_ceiling &&
			next_store_block >= next_renewal_block
		{
			wait_for_block_height(collator1, next_renewal_block, GEN_TIMEOUT_SECS)
				.await
				.with_context(|| {
					format!("Did not reach renewal pass block {}", next_renewal_block)
				})?;

			test_log!(
				TEST,
				"Renewal pass at block {}: renewing {} entries (RetentionPeriod={})",
				next_renewal_block,
				renewable_entries.len(),
				FIXTURE_RETENTION_PERIOD,
			);

			log::info!(
				"Starting renewal pass at block {} (nonce={})",
				next_renewal_block,
				bob_nonce_counter,
			);
			for entry in renewable_entries.iter_mut() {
				log::info!(
					"Renewing entry={}, block={}, index={}, bob_nonce={}",
					entry.entry,
					entry.latest_renewal_block,
					entry.latest_renewal_index,
					bob_nonce_counter,
				);
				let outcome = renew_data_with_hash(
					&collator_client,
					entry.latest_renewal_block,
					entry.latest_renewal_index,
					bob_nonce_counter,
				)
				.await?;
				bob_nonce_counter += 1;

				let expected_content_hash = hex::decode(&entry.content_hash)
					.with_context(|| format!("Invalid manifest hash for entry {}", entry.entry))?;
				anyhow::ensure!(
					expected_content_hash.as_slice() == outcome.content_hash.as_slice(),
					"renewed content hash mismatch for entry {}",
					entry.entry,
				);

				log::info!(
					"Renewed: was block {}, now block {} index {} (expires ~block {})",
					entry.latest_renewal_block,
					outcome.renewed_at_block,
					outcome.renewed_index,
					outcome.renewed_at_block + FIXTURE_RETENTION_PERIOD as u64 + 1,
				);
				entry.latest_renewal_block = outcome.renewed_at_block;
				entry.latest_renewal_index = outcome.renewed_index;
				wait_for_block_height(collator1, outcome.renewed_at_block + 2, GEN_TIMEOUT_SECS)
					.await?;
			}

			log::info!(
				"Renewal pass at {} complete ({} entries renewed)",
				next_renewal_block,
				renewable_entries.len(),
			);
			next_renewal_block += cfg.renewal_pass_interval;
		}

		wait_for_block_height(collator1, next_store_block, GEN_TIMEOUT_SECS)
			.await
			.with_context(|| format!("Collator did not reach block {}", next_store_block))?;

		let test_data = test_data_for_store_target_block(next_store_block);

		let is_renewable = store_count + 1 <= cfg.renewable_store_count;
		let block_num = if is_renewable {
			store_data_finalized(&collator_client, &test_data, nonce).await?
		} else {
			store_data(&collator_client, &test_data, nonce).await?
		};
		nonce += 1;
		store_count += 1;

		if is_renewable {
			let entry = store_count - 1;
			let content_hash = blake2_256(&test_data);
			renewable_entries.push(RenewableEntryManifest {
				entry,
				original_store_target_block: next_store_block,
				original_block: block_num,
				latest_renewal_block: block_num,
				latest_renewal_index: 0,
				content_hash: hex::encode(content_hash),
				cid: hash_to_cid(&content_hash),
			});
			log::info!(
				"Tracked renewable entry {}/{}: target block {}, finalized at block {}",
				store_count,
				cfg.renewable_store_count,
				next_store_block,
				block_num,
			);
		}

		if store_count % 5 == 0 {
			log::info!(
				"Store {}/{}: {} bytes at block {} (target was {})",
				store_count,
				cfg.target_blocks / cfg.store_interval,
				TEST_DATA_SIZE,
				block_num,
				next_store_block,
			);
		}

		next_store_block += cfg.store_interval;
	}

	anyhow::ensure!(
		!renewable_entries.is_empty(),
		"BUG: no renewable entries recorded (store_count={})",
		store_count,
	);
	anyhow::ensure!(
		next_renewal_block > cfg.renewal_pass_block,
		"BUG: renewal pass was never executed (store_count={}, next_renewal_block={})",
		store_count,
		next_renewal_block,
	);

	log::info!("All {} stores and {} renewals complete", store_count, renewable_entries.len(),);

	// === Phase 3: Wait for both nodes to reach target + finality ===
	test_log!(TEST, "Phase 3: Waiting for block {} finalized on both nodes", cfg.target_blocks);

	wait_for_finalized_height(collator1, cfg.target_blocks, GEN_TIMEOUT_SECS)
		.await
		.context("Collator did not finalize target block")?;
	log::info!("✓ Collator finalized block {}", cfg.target_blocks);

	let pruned_node = network.get_node("pruned-node").context("Failed to get pruned-node")?;
	wait_for_block_height(pruned_node, cfg.target_blocks, SYNC_TIMEOUT_SECS)
		.await
		.context("Pruned node did not reach target block")?;
	wait_for_finalized_height(pruned_node, cfg.target_blocks, GEN_TIMEOUT_SECS)
		.await
		.context("Pruned node did not finalize target block")?;
	log::info!("✓ Pruned node finalized block {}", cfg.target_blocks);

	// === Phase 4: Archive databases as .tgz snapshots ===
	test_log!(TEST, "Phase 4: Archiving databases to {}", output_dir.display());

	let base_dir = network
		.base_dir()
		.ok_or_else(|| anyhow!("Failed to get network base directory"))?
		.to_string();

	let collator_base = Path::new(&base_dir).join("collator-1");
	let pruned_base = Path::new(&base_dir).join("pruned-node");
	let relay_alice_base = Path::new(&base_dir).join("alice");

	let archive_tgz = output_dir.join("archive.tgz");
	let pruned_tgz = output_dir.join("pruned.tgz");
	let relay_tgz = output_dir.join("relay.tgz");

	// Save the raw parachain chain spec that zombienet generated for this run.
	// Zombienet customizes the genesis (e.g. registered collators) based on the network
	// topology, so the raw spec must be reused when loading snapshots — otherwise new
	// nodes would compute a different genesis hash and refuse to sync.
	let raw_chain_spec_src = collator_base.join("cfg").join(format!("{}.json", PARA_ID));
	let raw_chain_spec_dst = output_dir.join("raw-chain-spec.json");
	std::fs::copy(&raw_chain_spec_src, &raw_chain_spec_dst).with_context(|| {
		format!(
			"Failed to copy raw chain spec from {} to {}",
			raw_chain_spec_src.display(),
			raw_chain_spec_dst.display()
		)
	})?;
	log::info!("Saved raw parachain chain spec: {}", raw_chain_spec_dst.display());

	// Also save the relay chain spec — the relay genesis depends on the validator set,
	// so it must match when loading snapshots with a different number of validators.
	let raw_relay_spec_src = relay_alice_base.join("cfg").join("westend-local.json");
	let raw_relay_spec_dst = output_dir.join("raw-relay-chain-spec.json");
	std::fs::copy(&raw_relay_spec_src, &raw_relay_spec_dst).with_context(|| {
		format!(
			"Failed to copy relay chain spec from {} to {}",
			raw_relay_spec_src.display(),
			raw_relay_spec_dst.display()
		)
	})?;
	log::info!("Saved raw relay chain spec: {}", raw_relay_spec_dst.display());

	let manifest = SnapshotManifest {
		target_blocks: cfg.target_blocks,
		store_interval: cfg.store_interval,
		retention_period: FIXTURE_RETENTION_PERIOD,
		renewable_store_count: cfg.renewable_store_count,
		entries: renewable_entries.clone(),
	};
	let archive_manifest = archive_manifest_path(&output_dir);
	let manifest_file = std::fs::File::create(&archive_manifest)
		.with_context(|| format!("Failed to create {}", archive_manifest.display()))?;
	serde_json::to_writer_pretty(manifest_file, &manifest)
		.with_context(|| format!("Failed to write {}", archive_manifest.display()))?;
	log::info!("Saved archive manifest: {}", archive_manifest.display());

	// Collator snapshot includes both parachain data/ AND embedded relay-data/ so that
	// when restored the collator's embedded relay chain state matches the parachain state.
	log::info!("Creating archive snapshot (with relay-data): {}", archive_tgz.display());
	let archive_size = create_db_snapshot_tgz(&collator_base, &archive_tgz, &["relay-data"])
		.context("Failed to archive collator DB")?;

	log::info!("Creating pruned snapshot: {}", pruned_tgz.display());
	let pruned_size = create_db_snapshot_tgz(&pruned_base, &pruned_tgz, &[])
		.context("Failed to archive pruned DB")?;

	// Relay chain snapshot from alice — needed so relay validators start with matching state
	// instead of from genesis (which would cause the parachain collator to be unable to
	// produce blocks because the relay chain wouldn't recognize the parachain's history).
	log::info!("Creating relay chain snapshot: {}", relay_tgz.display());
	let relay_size = create_db_snapshot_tgz(&relay_alice_base, &relay_tgz, &[])
		.context("Failed to archive relay chain DB")?;

	log::info!(
		"✓ Archive snapshot: {} ({:.1} MB)",
		archive_tgz.display(),
		archive_size as f64 / 1_048_576.0
	);
	log::info!(
		"✓ Pruned snapshot: {} ({:.1} MB)",
		pruned_tgz.display(),
		pruned_size as f64 / 1_048_576.0
	);
	log::info!(
		"✓ Relay snapshot: {} ({:.1} MB)",
		relay_tgz.display(),
		relay_size as f64 / 1_048_576.0
	);

	test_log!(
		TEST,
		"=== Database generation complete: {} blocks, {} stores, {} renewals, archive={:.1}MB, pruned={:.1}MB, relay={:.1}MB ===",
		cfg.target_blocks,
		store_count,
		renewable_entries.len(),
		archive_size as f64 / 1_048_576.0,
		pruned_size as f64 / 1_048_576.0,
		relay_size as f64 / 1_048_576.0,
	);

	network.destroy().await?;
	Ok(())
}
