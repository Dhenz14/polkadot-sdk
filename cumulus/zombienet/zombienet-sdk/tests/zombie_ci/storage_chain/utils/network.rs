// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

use super::config::*;
use anyhow::{anyhow, Result};
use std::path::PathBuf;
use zombienet_sdk::{NetworkConfig, NetworkConfigBuilder};

pub fn verify_parachain_binaries() -> Result<()> {
	log::info!("Relay binary: {} (resolved via PATH)", RELAY_BINARY);
	log::info!("Parachain binary: {} (resolved via PATH)", PARACHAIN_BINARY);
	log::info!("Chain spec: {}", PARACHAIN_CHAIN_SPEC);
	if !PathBuf::from(PARACHAIN_CHAIN_SPEC).exists() {
		anyhow::bail!("Chain spec fixture not found at '{}'", PARACHAIN_CHAIN_SPEC);
	}
	Ok(())
}

/// Snapshots for a parachain network: collator DB + optional relay chain DB.
pub struct ParachainSnapshots<'a> {
	/// Collator snapshot (parachain data/ and optionally relay-data/).
	pub collator: &'a str,
	/// Relay chain snapshot for validators.
	pub relay: &'a str,
	/// Raw parachain chain spec saved from the gen-db run. Zombienet customizes genesis
	/// based on network topology (e.g. registered collators), so the exact raw spec must
	/// be reused when loading snapshots to ensure matching genesis hashes.
	pub chain_spec: &'a str,
	/// Raw relay chain spec saved from the gen-db run. Same reason as above — the relay
	/// genesis depends on the validator set, so it must match the snapshot.
	pub relay_chain_spec: &'a str,
}

/// Same as above but with optional DB snapshots for relay validators and collator.
/// Alice and bob get the relay snapshot; charlie starts fresh and syncs from them.
pub fn build_parachain_network_config_three_relay_validators_with_snapshots(
	para_node_args: Vec<String>,
	snapshots: Option<ParachainSnapshots>,
) -> Result<NetworkConfig> {
	let relay_binary = RELAY_BINARY.to_string();
	let para_binary = PARACHAIN_BINARY.to_string();
	let para_chain_spec = match &snapshots {
		Some(s) if !s.chain_spec.is_empty() => s.chain_spec.to_string(),
		_ => PARACHAIN_CHAIN_SPEC.to_string(),
	};

	log::info!("Relay binary: {}", relay_binary);
	log::info!("Parachain binary: {}", para_binary);
	log::info!("Parachain chain spec: {}", para_chain_spec);
	if let Some(ref snaps) = snapshots {
		log::info!("Collator DB snapshot: {}", snaps.collator);
		if !snaps.relay.is_empty() {
			log::info!("Relay DB snapshot: {}", snaps.relay);
		}
	}

	let relay_args: Vec<_> = vec!["-lruntime=debug"].into_iter().map(|s| s.into()).collect();
	let relay_args2 = relay_args.clone();
	let relay_args3 = relay_args.clone();

	let para_args: Vec<_> = para_node_args.iter().map(|s| s.as_str().into()).collect();

	let relay_chain = RELAY_CHAIN.to_string();
	let para_id = PARA_ID;
	log::info!("Relay chain: {}", relay_chain);
	log::info!("Parachain ID: {}", para_id);

	let relay_snapshot =
		snapshots.as_ref().filter(|s| !s.relay.is_empty()).map(|s| s.relay.to_string());
	let relay_snapshot2 = relay_snapshot.clone();
	let relay_snapshot3 = relay_snapshot.clone();
	let collator_snapshot = snapshots.as_ref().map(|s| s.collator.to_string());
	let relay_chain_spec_override = snapshots
		.as_ref()
		.filter(|s| !s.relay_chain_spec.is_empty())
		.map(|s| s.relay_chain_spec.to_string());

	NetworkConfigBuilder::new()
		.with_relaychain(|relaychain| {
			let r = relaychain
				.with_chain(relay_chain.as_str())
				.with_default_command(relay_binary.as_str());
			let r = match &relay_chain_spec_override {
				Some(spec) => r.with_chain_spec_path(spec.as_str()),
				None => r,
			};
			r.with_validator(|node| {
				let n = node.with_name("alice").validator(true).with_args(relay_args);
				match &relay_snapshot {
					Some(snap) => n.with_db_snapshot(snap.as_str()),
					None => n,
				}
			})
			.with_validator(|node| {
				let n = node.with_name("bob").validator(true).with_args(relay_args2);
				match &relay_snapshot2 {
					Some(snap) => n.with_db_snapshot(snap.as_str()),
					None => n,
				}
			})
			.with_validator(|node| {
				let n = node.with_name("charlie").validator(true).with_args(relay_args3);
				match &relay_snapshot3 {
					Some(snap) => n.with_db_snapshot(snap.as_str()),
					None => n,
				}
			})
		})
		.with_parachain(|parachain| {
			parachain
				.with_id(para_id)
				.with_chain_spec_path(para_chain_spec.as_str())
				.cumulus_based(true)
				.with_collator(|c| {
					let c = c
						.with_name("collator-1")
						.validator(true)
						.with_command(para_binary.as_str())
						.with_args(para_args);
					match &collator_snapshot {
						Some(snap) => c.with_db_snapshot(snap.as_str()),
						None => c,
					}
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
