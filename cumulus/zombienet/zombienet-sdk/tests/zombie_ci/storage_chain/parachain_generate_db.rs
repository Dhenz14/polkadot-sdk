// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Database generation test for parachain sync tests.
//!
//! This test generates two pre-built databases that future sync tests can use
//! as starting points, avoiding the need to produce blocks from genesis every time.
//!
//! ## What it produces
//!
//! - **Archive DB**: A collator node with no pruning, containing all 1000 blocks with indexed
//!   transaction data stored every 10 blocks (100 stores total).
//! - **Pruned DB**: A full-sync node with `--blocks-pruning=100`, synced from the archive collator.
//!   Old blocks beyond the pruning window are deleted.
//!
//! ## How it works
//!
//! 1. Spawns a parachain network: 2 relay validators + 1 archive collator + 1 pruned full node
//! 2. Waits for session change so the parachain starts producing blocks
//! 3. Authorizes Alice for 150 store transactions upfront
//! 4. Every 10 blocks, stores 2KB of unique test data via the collator
//! 5. Waits for both nodes to reach block 1000 (finalized)
//! 6. Copies parachain databases to `$DB_OUTPUT_DIR` (default: `./zombienet/test-databases/`)
//!
//! ## Environment Variables
//!
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
		generate_test_data, get_alice_nonce,
		initialize_network,
		renew_data, set_retention_period, verify_parachain_binaries, wait_for_block_height,
		wait_for_finalized_height, wait_for_session_change_on_node,
		BLOCK_PRODUCTION_TIMEOUT_SECS, NETWORK_READY_TIMEOUT_SECS, NODE_LOG_CONFIG, PARA_ID, PARACHAIN_BINARY, PARACHAIN_CHAIN_SPEC, RELAY_BINARY, RELAY_CHAIN,
		SYNC_TIMEOUT_SECS, TEST_DATA_SIZE,
};
use crate::test_log;
use anyhow::{anyhow, Context, Result};
use env_logger::Env;
use flate2::{write::GzEncoder, Compression};
use std::path::{Path, PathBuf};
use zombienet_sdk::subxt::{
	config::substrate::{SubstrateConfig, SubstrateExtrinsicParamsBuilder},
	dynamic::{tx, Value},
	ext::scale_value::value,
	OnlineClient,
};
use zombienet_sdk::subxt_signer::sr25519::dev;
use zombienet_sdk::{NetworkConfig, NetworkConfigBuilder};

const TARGET_BLOCKS: u64 = 1000;
const STORE_INTERVAL: u64 = 10;
const RETENTION_PERIOD: u32 = 200;
const PRUNING_BLOCKS: u32 = RETENTION_PERIOD;
const SESSION_CHANGE_TIMEOUT_SECS: u64 = 300;
const RENEWABLE_STORE_COUNT: u64 = 10;
const RENEWAL_PASS_BLOCK: u64 = 105;
const RENEWAL_PASS_INTERVAL: u64 = 80;
const LAST_RENEWAL_PASS_CEILING: u64 = TARGET_BLOCKS.saturating_sub(30);
const DB_OUTPUT_DIR_ENV: &str = "DB_OUTPUT_DIR";
const DEFAULT_DB_OUTPUT_DIR: &str = "./zombienet/test-databases";
/// How many store transactions to authorize upfront.
/// We store once every STORE_INTERVAL blocks across TARGET_BLOCKS, so
/// TARGET_BLOCKS / STORE_INTERVAL = 50 stores. Add margin for safety.
const AUTHORIZE_TRANSACTIONS: u32 = 100;
/// Authorize enough bytes for all stores (each store is TEST_DATA_SIZE bytes).
const AUTHORIZE_BYTES: u64 = (AUTHORIZE_TRANSACTIONS as u64) * (TEST_DATA_SIZE as u64) * 2;

/// Timeout for reaching TARGET_BLOCKS. Parachain blocks are ~12s each in local
/// testnet, so 500 blocks ≈ 6000s. Add generous margin.
const GEN_TIMEOUT_SECS: u64 = 10000;

fn get_db_output_dir() -> PathBuf {
	let dir = std::env::var(DB_OUTPUT_DIR_ENV).unwrap_or_else(|_| DEFAULT_DB_OUTPUT_DIR.into());
	PathBuf::from(dir)
}

fn build_gendb_network_config() -> Result<NetworkConfig> {
	let relay_binary = RELAY_BINARY.to_string();
	let para_binary = PARACHAIN_BINARY.to_string();
	let para_chain_spec = PARACHAIN_CHAIN_SPEC.to_string();
	let relay_chain = RELAY_CHAIN.to_string();
	let para_id = PARA_ID;

	let relay_args: Vec<_> = vec!["-lruntime=debug"].into_iter().map(|s| s.into()).collect();
	let relay_args2 = relay_args.clone();

	let collator_args: Vec<_> = vec!["--ipfs-server", NODE_LOG_CONFIG]
		.into_iter()
		.map(|s| s.into())
		.collect();

	let pruning_flag = format!("--blocks-pruning={}", PRUNING_BLOCKS);
	let pruned_args: Vec<_> = vec![
		"--sync=full",
		"--ipfs-server",
		pruning_flag.as_str(),
		NODE_LOG_CONFIG,
	]
	.into_iter()
	.map(|s| s.into())
	.collect();

	NetworkConfigBuilder::new()
		.with_relaychain(|relaychain| {
			relaychain
				.with_chain(relay_chain.as_str())
				.with_default_command(relay_binary.as_str())
				.with_node(|node| node.with_name("alice").validator(true).with_args(relay_args))
				.with_node(|node| node.with_name("bob").validator(true).with_args(relay_args2))
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
async fn authorize_bulk_storage(client: &OnlineClient<SubstrateConfig>, nonce: u64) -> Result<()> {
	let signer = dev::alice();

	let authorize_call = zombienet_sdk::subxt::tx::dynamic(
		"Sudo",
		"sudo",
		vec![value! {
			TransactionStorage(authorize_account {
				who: Value::from_bytes(signer.public_key().0),
				transactions: AUTHORIZE_TRANSACTIONS,
				bytes: AUTHORIZE_BYTES
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
		AUTHORIZE_TRANSACTIONS,
		AUTHORIZE_BYTES,
	);
	Ok(())
}

async fn authorize_bob_for_renewals(
	client: &OnlineClient<SubstrateConfig>,
	nonce: u64,
) -> Result<()> {
	let signer = dev::alice();
	let bob = dev::bob();

	let authorize_call = zombienet_sdk::subxt::tx::dynamic(
		"Sudo",
		"sudo",
		vec![value! {
			TransactionStorage(authorize_account {
				who: Value::from_bytes(bob.public_key().0),
				transactions: AUTHORIZE_TRANSACTIONS,
				bytes: AUTHORIZE_BYTES
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

	log::info!("Authorized Bob for {} transactions / {} bytes", AUTHORIZE_TRANSACTIONS, AUTHORIZE_BYTES);
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

	test_log!(TEST, "=== Parachain Database Generation ===");
	log::info!(
		"Generating archive + pruned databases ({} blocks, store every {} blocks, pruning={})",
		TARGET_BLOCKS,
		STORE_INTERVAL,
		PRUNING_BLOCKS,
	);

	verify_parachain_binaries()?;

	let output_dir = get_db_output_dir();
	log::info!("Output directory: {}", output_dir.display());

	// === Phase 1: Spawn network (archive collator + pruned full node) ===
	test_log!(
		TEST,
		"Phase 1: Spawning network (collator + pruned-node with --blocks-pruning={})",
		PRUNING_BLOCKS
	);

	let config = build_gendb_network_config()?;
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
		STORE_INTERVAL,
		TARGET_BLOCKS
	);

	let collator1 = network.get_node("collator-1").context("Failed to get collator-1 node")?;
	let collator_client: OnlineClient<SubstrateConfig> = collator1.wait_client().await?;

	let mut nonce = get_alice_nonce(collator1).await?;

	set_retention_period(&collator_client, RETENTION_PERIOD, nonce).await?;
	nonce += 1;

	authorize_bulk_storage(&collator_client, nonce).await?;
	nonce += 1;

	authorize_bob_for_renewals(&collator_client, nonce).await?;
	nonce += 1;

	// Wait for parachain to start producing and track current height
	wait_for_block_height(collator1, 1, BLOCK_PRODUCTION_TIMEOUT_SECS).await?;

	let mut renewable_entries: Vec<(u64, u32)> = Vec::new();
	let mut next_renewal_block: u64 = RENEWAL_PASS_BLOCK;
	let mut bob_nonce_counter: u64 = 0;
	let mut next_store_block: u64 = STORE_INTERVAL;
	let mut store_count: u64 = 0;

	while next_store_block <= TARGET_BLOCKS {
		let renewal_ready = !renewable_entries.is_empty() ||
			(store_count >= RENEWABLE_STORE_COUNT &&
				next_store_block > RENEWABLE_STORE_COUNT * STORE_INTERVAL);
		if renewal_ready &&
			next_renewal_block <= LAST_RENEWAL_PASS_CEILING &&
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
				RETENTION_PERIOD,
			);

			log::info!(
				"Starting renewal pass at block {} (nonce={})",
				next_renewal_block, bob_nonce_counter,
			);
			for entry in renewable_entries.iter_mut() {
				log::info!(
					"Renewing block={}, index={}, bob_nonce={}",
					entry.0, entry.1, bob_nonce_counter,
				);
				let renew_block =
					renew_data(&collator_client, entry.0, entry.1, bob_nonce_counter).await?;
				bob_nonce_counter += 1;
				log::info!(
					"Renewed: was block {}, now block {} (expires ~block {})",
					entry.0,
					renew_block,
					renew_block + RETENTION_PERIOD as u64 + 1,
				);
				entry.0 = renew_block;
				wait_for_block_height(collator1, renew_block + 2, GEN_TIMEOUT_SECS).await?;
			}

			log::info!(
				"Renewal pass at {} complete ({} entries renewed)",
				next_renewal_block,
				renewable_entries.len(),
			);
			next_renewal_block += RENEWAL_PASS_INTERVAL;
		}

		wait_for_block_height(collator1, next_store_block, GEN_TIMEOUT_SECS)
			.await
			.with_context(|| format!("Collator did not reach block {}", next_store_block))?;

		let pattern = format!("PARA_GENDB_{:04}_", next_store_block);
		let test_data = generate_test_data(TEST_DATA_SIZE, pattern.as_bytes());

		let is_renewable = store_count + 1 <= RENEWABLE_STORE_COUNT;
		let block_num = if is_renewable {
			store_data_finalized(&collator_client, &test_data, nonce).await?
		} else {
			store_data(&collator_client, &test_data, nonce).await?
		};
		nonce += 1;
		store_count += 1;

		if is_renewable {
			renewable_entries.push((block_num, 0));
			log::info!(
				"Tracked renewable entry {}/{}: finalized at block {}",
				store_count,
				RENEWABLE_STORE_COUNT,
				block_num,
			);
		}

		if store_count % 5 == 0 {
			log::info!(
				"Store {}/{}: {} bytes at block {} (target was {})",
				store_count,
				TARGET_BLOCKS / STORE_INTERVAL,
				TEST_DATA_SIZE,
				block_num,
				next_store_block,
			);
		}

		next_store_block += STORE_INTERVAL;
	}

	anyhow::ensure!(
		!renewable_entries.is_empty(),
		"BUG: no renewable entries recorded (store_count={})",
		store_count,
	);
	anyhow::ensure!(
		next_renewal_block > RENEWAL_PASS_BLOCK,
		"BUG: renewal pass was never executed (store_count={}, next_renewal_block={})",
		store_count,
		next_renewal_block,
	);

	log::info!(
		"All {} stores and {} renewals complete",
		store_count,
		renewable_entries.len(),
	);

	// === Phase 3: Wait for both nodes to reach target + finality ===
	test_log!(TEST, "Phase 3: Waiting for block {} finalized on both nodes", TARGET_BLOCKS);

	wait_for_finalized_height(collator1, TARGET_BLOCKS, GEN_TIMEOUT_SECS)
		.await
		.context("Collator did not finalize target block")?;
	log::info!("✓ Collator finalized block {}", TARGET_BLOCKS);

	let pruned_node = network.get_node("pruned-node").context("Failed to get pruned-node")?;
	wait_for_block_height(pruned_node, TARGET_BLOCKS, SYNC_TIMEOUT_SECS)
		.await
		.context("Pruned node did not reach target block")?;
	wait_for_finalized_height(pruned_node, TARGET_BLOCKS, GEN_TIMEOUT_SECS)
		.await
		.context("Pruned node did not finalize target block")?;
	log::info!("✓ Pruned node finalized block {}", TARGET_BLOCKS);

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
		TARGET_BLOCKS,
		store_count,
		renewable_entries.len(),
		archive_size as f64 / 1_048_576.0,
		pruned_size as f64 / 1_048_576.0,
		relay_size as f64 / 1_048_576.0,
	);

	network.destroy().await?;
	Ok(())
}
