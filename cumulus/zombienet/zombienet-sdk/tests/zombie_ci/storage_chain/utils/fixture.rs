// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Shared storage-chain snapshot fixture layout and manifest helpers.

use super::{generate_test_data, ParachainSnapshots, TEST_DATA_SIZE};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
#[cfg(feature = "generate-snapshots")]
use std::path::Path;
use std::path::PathBuf;

pub const FIXTURE_RETENTION_PERIOD: u32 = 200;
pub const SNAPSHOT_STORE_INTERVAL: u64 = 10;
pub const TIP_SYNC_TARGET_BLOCKS: u64 = 300;
pub const TIP_SYNC_RENEWABLE_STORE_COUNT: u64 = 10;

#[cfg(feature = "generate-snapshots")]
pub const ARCHIVE_MANIFEST_FILE: &str = "archive-manifest.json";

pub const TIP_SYNC_SNAPSHOT_ENV: &str = "STORAGE_CHAIN_TIP_SYNC_SNAPSHOT";
pub const RELAY_SNAPSHOT_ENV: &str = "STORAGE_CHAIN_RELAY_SNAPSHOT";
pub const RAW_CHAIN_SPEC_ENV: &str = "STORAGE_CHAIN_RAW_CHAIN_SPEC";
pub const RAW_RELAY_CHAIN_SPEC_ENV: &str = "STORAGE_CHAIN_RAW_RELAY_CHAIN_SPEC";
pub const TIP_SYNC_MANIFEST_ENV: &str = "STORAGE_CHAIN_TIP_SYNC_MANIFEST";

// Placeholder URLs until the storage-chain fixture bucket exists. Local runs should set the
// matching snapshot/manifest overrides above to point at freshly generated fixtures. Chain specs
// are checked into this repository and intentionally do not use GCS defaults.
const DEFAULT_TIP_SYNC_SNAPSHOT: &str =
	"https://storage.googleapis.com/fake-storage-chain-fixtures/tip-sync-300.tgz";
const DEFAULT_RELAY_SNAPSHOT: &str =
	"https://storage.googleapis.com/fake-storage-chain-fixtures/relay.tgz";
const DEFAULT_TIP_SYNC_MANIFEST: &str =
	"https://storage.googleapis.com/fake-storage-chain-fixtures/tip-sync-300-manifest.json";

const SNAPSHOT_DIR: &str = "tests/zombie_ci/storage_chain/fixtures/test-databases";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotManifest {
	pub target_blocks: u64,
	pub store_interval: u64,
	pub retention_period: u32,
	pub renewable_store_count: u64,
	pub entries: Vec<RenewableEntryManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenewableEntryManifest {
	pub entry: u64,
	pub original_store_target_block: u64,
	pub original_block: u64,
	pub latest_renewal_block: u64,
	pub latest_renewal_index: u32,
	pub content_hash: String,
	pub cid: String,
}

pub struct ResolvedSnapshots {
	pub collator: PathBuf,
	pub relay: PathBuf,
	pub chain_spec: PathBuf,
	pub relay_chain_spec: PathBuf,
	pub manifest: PathBuf,
}

impl ResolvedSnapshots {
	pub fn load() -> Result<Self> {
		let collator =
			fixture_from_env_or_default(TIP_SYNC_SNAPSHOT_ENV, DEFAULT_TIP_SYNC_SNAPSHOT);
		let relay = fixture_from_env_or_default(RELAY_SNAPSHOT_ENV, DEFAULT_RELAY_SNAPSHOT);
		let chain_spec = fixture_from_env_or_local(RAW_CHAIN_SPEC_ENV, raw_chain_spec_path())?;
		let relay_chain_spec =
			fixture_from_env_or_local(RAW_RELAY_CHAIN_SPEC_ENV, raw_relay_chain_spec_path())?;
		let manifest =
			fixture_from_env_or_default(TIP_SYNC_MANIFEST_ENV, DEFAULT_TIP_SYNC_MANIFEST);

		Ok(Self { collator, relay, chain_spec, relay_chain_spec, manifest })
	}

	pub fn as_parachain_snapshots(&self) -> ParachainSnapshots<'_> {
		ParachainSnapshots {
			collator: self.collator.to_str().expect("non-utf8 path"),
			relay: self.relay.to_str().expect("non-utf8 path"),
			chain_spec: self.chain_spec.to_str().expect("non-utf8 path"),
			relay_chain_spec: self.relay_chain_spec.to_str().expect("non-utf8 path"),
		}
	}

	pub fn load_manifest(&self) -> Result<SnapshotManifest> {
		let file = std::fs::File::open(&self.manifest)
			.with_context(|| format!("Failed to open {}", self.manifest.display()))?;
		serde_json::from_reader(file)
			.with_context(|| format!("Failed to decode {}", self.manifest.display()))
	}
}

#[cfg(feature = "generate-snapshots")]
pub fn archive_manifest_path(output_dir: &Path) -> PathBuf {
	output_dir.join(ARCHIVE_MANIFEST_FILE)
}

pub fn fixture_snapshot_dir() -> PathBuf {
	PathBuf::from(SNAPSHOT_DIR)
}

pub fn raw_chain_spec_path() -> PathBuf {
	fixture_snapshot_dir().join("raw-chain-spec.json")
}

pub fn raw_relay_chain_spec_path() -> PathBuf {
	fixture_snapshot_dir().join("raw-relay-chain-spec.json")
}

fn fixture_from_env_or_default(env_var: &str, default_url: &str) -> PathBuf {
	std::env::var(env_var)
		.map(PathBuf::from)
		.unwrap_or_else(|_| PathBuf::from(default_url))
}

fn fixture_from_env_or_local(env_var: &str, local_path: PathBuf) -> Result<PathBuf> {
	match std::env::var(env_var) {
		Ok(path) => Ok(PathBuf::from(path)),
		Err(_) => std::fs::canonicalize(&local_path).with_context(|| {
			format!("checked-in chain spec fixture not found: {}", local_path.display(),)
		}),
	}
}

pub fn test_data_for_store_target_block(block: u64) -> Vec<u8> {
	let pattern = format!("PARA_GENDB_{block:04}_");
	generate_test_data(TEST_DATA_SIZE, pattern.as_bytes())
}

pub fn renewable_entry_data(entry: u64) -> Vec<u8> {
	let original_store_target_block = (entry + 1) * SNAPSHOT_STORE_INTERVAL;
	test_data_for_store_target_block(original_store_target_block)
}
