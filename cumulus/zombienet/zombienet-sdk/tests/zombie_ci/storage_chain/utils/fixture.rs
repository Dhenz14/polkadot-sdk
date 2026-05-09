// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Shared storage-chain snapshot fixture layout and manifest helpers.

use super::{blake2_256, generate_test_data, hash_to_cid, ParachainSnapshots, TEST_DATA_SIZE};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const FIXTURE_RETENTION_PERIOD: u32 = 200;
pub const SNAPSHOT_STORE_INTERVAL: u64 = 10;
pub const TIP_SYNC_TARGET_BLOCKS: u64 = 300;
pub const TIP_SYNC_RENEWABLE_STORE_COUNT: u64 = 10;

pub const ARCHIVE_MANIFEST_FILE: &str = "archive-manifest.json";
pub const TIP_SYNC_MANIFEST_FILE: &str = "tip-sync-300-manifest.json";

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
		let collator = canonicalize_fixture(tip_sync_snapshot_path(), "tip-sync-300.tgz")?;
		let relay = canonicalize_fixture(relay_snapshot_path(), "relay.tgz")?;
		let chain_spec = canonicalize_fixture(raw_chain_spec_path(), "raw-chain-spec.json")?;
		let relay_chain_spec =
			canonicalize_fixture(raw_relay_chain_spec_path(), "raw-relay-chain-spec.json")?;
		let manifest = canonicalize_fixture(tip_sync_manifest_path(), TIP_SYNC_MANIFEST_FILE)?;

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

pub fn fixture_snapshot_dir() -> PathBuf {
	PathBuf::from(SNAPSHOT_DIR)
}

pub fn tip_sync_snapshot_path() -> PathBuf {
	fixture_snapshot_dir().join("tip-sync-300.tgz")
}

pub fn tip_sync_manifest_path() -> PathBuf {
	fixture_snapshot_dir().join(TIP_SYNC_MANIFEST_FILE)
}

pub fn archive_manifest_path(output_dir: &Path) -> PathBuf {
	output_dir.join(ARCHIVE_MANIFEST_FILE)
}

pub fn relay_snapshot_path() -> PathBuf {
	fixture_snapshot_dir().join("relay.tgz")
}

pub fn raw_chain_spec_path() -> PathBuf {
	fixture_snapshot_dir().join("raw-chain-spec.json")
}

pub fn raw_relay_chain_spec_path() -> PathBuf {
	fixture_snapshot_dir().join("raw-relay-chain-spec.json")
}

pub fn test_data_for_store_target_block(block: u64) -> Vec<u8> {
	let pattern = format!("PARA_GENDB_{block:04}_");
	generate_test_data(TEST_DATA_SIZE, pattern.as_bytes())
}

pub fn renewable_entry_data(entry: u64) -> Vec<u8> {
	let original_store_target_block = (entry + 1) * SNAPSHOT_STORE_INTERVAL;
	test_data_for_store_target_block(original_store_target_block)
}

pub fn renewable_entry_content_hash(entry: u64) -> [u8; 32] {
	blake2_256(&renewable_entry_data(entry))
}

pub fn renewable_entry_cid(entry: u64) -> String {
	hash_to_cid(&renewable_entry_content_hash(entry))
}

fn canonicalize_fixture(path: PathBuf, file_name: &str) -> Result<PathBuf> {
	std::fs::canonicalize(&path).with_context(|| {
		format!(
			"{} not found in {}. Generate storage-chain fixtures and copy archive outputs into the tip-sync fixture names.",
			file_name,
			fixture_snapshot_dir().display(),
		)
	})
}
