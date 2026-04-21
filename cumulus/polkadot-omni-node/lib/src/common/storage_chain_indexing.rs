// Copyright (C) Parity Technologies (UK) Ltd.
// This file is part of Cumulus.
// SPDX-License-Identifier: Apache-2.0

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Storage-chain indexing bootstrap task.
//!
//! Populates `BODY_INDEX` + `TRANSACTION` columns for blocks inside the
//! post-warp-sync retention window so the node looks like a full-sync node to
//! the bitswap server.
//!
//! **Algorithm** (one-shot; task exits after completion):
//!
//! 1. BootWait — poll `SyncingService::status()` until `state == Idle` and
//!    `warp_sync.is_none()` (gap sync finished).
//! 2. CommitSweep — per block in ascending order within the retention window:
//!     1. Load runtime API metadata via `IndexedTransactionsApi` + local body.
//!     2. Call `Backend::apply_indexed_meta_for_block`, which commits
//!        `BODY_INDEX` and issues `store(TRANSACTION, h, data)` for each STORE
//!        match.
//!     3. For each content hash in the returned `missing` list (RENEW with no
//!        matching body tail), increment `pending_bumps[h]`.
//! 3. ResolvePending — for each `h` in `pending_bumps`:
//!     - If `has_indexed_transaction(h) == true` (stored by an earlier block in
//!       this sweep or by a previous session): call `bump_transaction_ref(h,
//!       count)` to bump the counter by `count` references.
//!     - Else: bitswap-fetch the data and call
//!       `store_fetched_transaction_with_count(h, data, count)` which issues
//!       one `store()` plus `count - 1` `reference()` calls.
//!
//! Post-sweep the task exits. Subsequent live block imports go through the
//! standard substrate execution path (`apply_index_ops`), which correctly
//! populates `BODY_INDEX` + `TRANSACTION` with proper ref-counting.
//!
//! **Invariant preserved:** for every content hash `h`,
//! `counter(TRANSACTION, h)` equals the number of `DbExtrinsic::Indexed {
//! hash: h, .. }` entries across all retained blocks' `BODY_INDEX`.

use futures::FutureExt;
use sc_client_api::{Backend as BackendT, BlockBackend, HeaderBackend};
use sc_client_db::{Backend, IndexedTransactionMeta};
use sp_blockchain::Backend as BlockchainBackendT;
use sc_network::{
	bitswap::{BitswapClient, BitswapError},
	NetworkRequest,
};
use sc_network_sync::{types::SyncState, SyncingService};
use sc_service::TaskManager;
use sp_api::{ApiExt, ProvideRuntimeApi};
use sp_runtime::traits::{
	Block as BlockT, Header as HeaderT, NumberFor, SaturatedConversion, UniqueSaturatedInto,
};
use sp_transaction_storage_proof::runtime_api::TransactionStorageApi;
use std::{collections::HashMap, sync::Arc, time::Duration};

use super::indexed_transactions_api::{
	HashingAlgorithm, IndexedTransactionInfo, IndexedTransactionsApi,
};

const LOG_TARGET: &str = "storage-chain-indexer";
const RAW_CID_CODEC: u64 = 0x55;
const BITSWAP_PER_PEER_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PEERS_PER_HASH: usize = 8;
const GAP_SYNC_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Returns `true` if the runtime at the current finalized state implements
/// [`IndexedTransactionsApi`]. When `false`, the indexer is a no-op.
pub(crate) fn runtime_supports_indexing<Block, Client>(client: &Client) -> bool
where
	Block: BlockT,
	Client: HeaderBackend<Block> + ProvideRuntimeApi<Block>,
	Client::Api: IndexedTransactionsApi<Block>,
{
	let finalized_hash = client.info().finalized_hash;
	client
		.runtime_api()
		.has_api::<dyn IndexedTransactionsApi<Block>>(finalized_hash)
		.unwrap_or(false)
}

/// Spawn the storage-chain indexer as a one-shot bootstrap task on
/// `task_manager`. Exits after CommitSweep + ResolvePending complete.
pub(crate) fn spawn<Block, Client, Net>(
	client: Arc<Client>,
	backend: Arc<Backend<Block>>,
	network: Arc<Net>,
	sync_service: Arc<SyncingService<Block>>,
	task_manager: &TaskManager,
	blocks_pruning: u32,
) where
	Block: BlockT<Hash = sc_client_db::DbHash>,
	NumberFor<Block>: UniqueSaturatedInto<u64>,
	Client: HeaderBackend<Block>
		+ BlockBackend<Block>
		+ ProvideRuntimeApi<Block>
		+ Send
		+ Sync
		+ 'static,
	Client::Api: IndexedTransactionsApi<Block> + TransactionStorageApi<Block>,
	Net: NetworkRequest + Send + Sync + ?Sized + 'static,
	Arc<Net>: Send + Sync + 'static,
{
	let task = run(client, backend, network, sync_service, blocks_pruning);
	task_manager
		.spawn_handle()
		.spawn("storage-chain-indexer", Some("storage-chain"), task.boxed());
}

async fn run<Block, Client, Net>(
	client: Arc<Client>,
	backend: Arc<Backend<Block>>,
	network: Arc<Net>,
	sync_service: Arc<SyncingService<Block>>,
	blocks_pruning: u32,
) where
	Block: BlockT<Hash = sc_client_db::DbHash>,
	NumberFor<Block>: UniqueSaturatedInto<u64>,
	Client: HeaderBackend<Block> + BlockBackend<Block> + ProvideRuntimeApi<Block>,
	Client::Api: IndexedTransactionsApi<Block> + TransactionStorageApi<Block>,
	Net: NetworkRequest + Send + Sync + ?Sized + 'static,
{
	log::info!(
		target: LOG_TARGET,
		"Waiting for gap sync to finish before running initial sweep (blocks_pruning={blocks_pruning})",
	);

	wait_for_gap_sync_complete(&sync_service).await;

	let finalized_hash = client.info().finalized_hash;
	let finalized_number: u64 = client
		.header(finalized_hash)
		.ok()
		.flatten()
		.map(|h| (*h.number()).unique_saturated_into())
		.unwrap_or(0);

	let retention_period: u64 = client
		.runtime_api()
		.retention_period(finalized_hash)
		.map(|n| n.unique_saturated_into())
		.unwrap_or(blocks_pruning as u64);
	let window_span = retention_period.max(blocks_pruning as u64);
	let window_start: u64 = finalized_number.saturating_sub(window_span);

	log::info!(
		target: LOG_TARGET,
		"InitialSweep starting, retention_period={retention_period}, \
		 window=[{window_start}..={finalized_number}]",
	);

	let (pending_bumps, meta_by_hash) = commit_sweep::<Block, Client>(
		&*client,
		&*backend,
		window_start,
		finalized_number,
		finalized_hash,
	);

	log::info!(
		target: LOG_TARGET,
		"CommitSweep done: {} content hashes need ResolvePending",
		pending_bumps.len(),
	);

	resolve_pending::<Block, Net>(
		&*backend,
		&*network,
		&*sync_service,
		&pending_bumps,
		&meta_by_hash,
	)
	.await;

	log::info!(target: LOG_TARGET, "sweep complete, exiting");
}

async fn wait_for_gap_sync_complete<Block: BlockT>(sync_service: &SyncingService<Block>) {
	loop {
		match sync_service.status().await {
			Ok(status) => {
				let idle = matches!(status.state, SyncState::Idle);
				let gap_done = status.warp_sync.is_none();
				log::debug!(
					target: LOG_TARGET,
					"gap-sync poll: state={:?}, warp_sync_some={}",
					status.state,
					status.warp_sync.is_some(),
				);
				if idle && gap_done {
					return;
				}
			},
			Err(_) => return,
		}
		futures_timer::Delay::new(GAP_SYNC_POLL_INTERVAL).await;
	}
}

fn commit_sweep<Block, Client>(
	client: &Client,
	backend: &Backend<Block>,
	window_start: u64,
	finalized_number: u64,
	state_hash: Block::Hash,
) -> (HashMap<[u8; 32], u32>, HashMap<[u8; 32], IndexedTransactionInfo>)
where
	Block: BlockT<Hash = sc_client_db::DbHash>,
	NumberFor<Block>: UniqueSaturatedInto<u64>,
	Client: HeaderBackend<Block> + BlockBackend<Block> + ProvideRuntimeApi<Block>,
	Client::Api: IndexedTransactionsApi<Block>,
{
	let mut pending_bumps: HashMap<[u8; 32], u32> = HashMap::new();
	let mut meta_by_hash: HashMap<[u8; 32], IndexedTransactionInfo> = HashMap::new();

	let api = client.runtime_api();

	for n in window_start..=finalized_number {
		let block_n: NumberFor<Block> = n.saturated_into();
		let block_hash = match client.hash(block_n) {
			Ok(Some(h)) => h,
			Ok(None) => continue,
			Err(e) => {
				log::warn!(target: LOG_TARGET, "block #{n}: hash lookup failed: {e}");
				continue;
			},
		};

		let meta = match api.indexed_transactions(state_hash, n as u32) {
			Ok(Some(m)) if !m.is_empty() => m,
			Ok(_) => continue,
			Err(e) => {
				log::warn!(target: LOG_TARGET, "block #{n}: runtime API failed: {e}");
				continue;
			},
		};

		for entry in &meta {
			if is_supported(entry) {
				meta_by_hash.entry(entry.content_hash).or_insert_with(|| entry.clone());
			}
		}

		let body_present = match client.block_body(block_hash) {
			Ok(v) => v.is_some(),
			Err(e) => {
				log::warn!(
					target: LOG_TARGET,
					"block #{n} ({block_hash:?}): body lookup failed: {e}",
				);
				continue;
			},
		};

		if !body_present {
			log::debug!(
				target: LOG_TARGET,
				"block #{n} ({block_hash:?}): no body; recording {} entries as pending refs",
				meta.len(),
			);
			for entry in &meta {
				if is_supported(entry) {
					*pending_bumps.entry(entry.content_hash).or_insert(0) += 1;
				}
			}
			continue;
		}

		let db_meta: Vec<IndexedTransactionMeta> = meta
			.iter()
			.filter(|m| is_supported(m))
			.map(|m| IndexedTransactionMeta { content_hash: m.content_hash, size: m.size })
			.collect();

		if db_meta.is_empty() {
			continue;
		}

		match backend.apply_indexed_meta_for_block(block_hash, block_n, db_meta) {
			Ok(missing) => {
				log::info!(
					target: LOG_TARGET,
					"block #{n} ({block_hash:?}): BODY_INDEX committed, {} entries deferred",
					missing.len(),
				);
				for h in missing {
					*pending_bumps.entry(h).or_insert(0) += 1;
				}
			},
			Err(e) => {
				log::warn!(
					target: LOG_TARGET,
					"block #{n} ({block_hash:?}): apply_indexed_meta_for_block failed: {e}",
				);
			},
		}
	}

	(pending_bumps, meta_by_hash)
}

async fn resolve_pending<Block, Net>(
	backend: &Backend<Block>,
	network: &Net,
	sync_service: &SyncingService<Block>,
	pending_bumps: &HashMap<[u8; 32], u32>,
	meta_by_hash: &HashMap<[u8; 32], IndexedTransactionInfo>,
) where
	Block: BlockT<Hash = sc_client_db::DbHash>,
	Net: NetworkRequest + ?Sized,
{
	for (content_hash, count) in pending_bumps {
		if *count == 0 {
			continue;
		}

		let already_stored = backend
			.blockchain()
			.has_indexed_transaction((*content_hash).into())
			.unwrap_or(false);

		if already_stored {
			if let Err(e) = backend.bump_transaction_ref(*content_hash, *count) {
				log::warn!(
					target: LOG_TARGET,
					"bump_transaction_ref({:?}, {}) failed: {e}",
					content_hash,
					count,
				);
			} else {
				log::info!(
					target: LOG_TARGET,
					"bumped {:?} by {} refs (already in TRANSACTION)",
					content_hash,
					count,
				);
			}
			continue;
		}

		let info = match meta_by_hash.get(content_hash) {
			Some(i) => i,
			None => continue,
		};
		if !is_supported(info) {
			continue;
		}

		match fetch_via_bitswap::<Block, Net>(network, sync_service, *content_hash).await {
			Some(data) => {
				if let Err(e) = backend.store_fetched_transaction_with_count(
					*content_hash,
					data,
					*count,
				) {
					log::warn!(
						target: LOG_TARGET,
						"store_fetched_transaction_with_count({:?}, {}) failed: {e}",
						content_hash,
						count,
					);
				} else {
					log::info!(
						target: LOG_TARGET,
						"bitswap-fetched {:?} stored with ref_count={}",
						content_hash,
						count,
					);
				}
			},
			None => {
				log::warn!(
					target: LOG_TARGET,
					"bitswap fetch for {:?} exhausted all peers without success (expected target_ref_count={})",
					content_hash,
					count,
				);
			},
		}
	}
}

fn is_supported(info: &IndexedTransactionInfo) -> bool {
	matches!(info.hashing, HashingAlgorithm::Blake2b256) && info.cid_codec == RAW_CID_CODEC
}

async fn fetch_via_bitswap<Block, Net>(
	network: &Net,
	sync_service: &SyncingService<Block>,
	content_hash: [u8; 32],
) -> Option<Vec<u8>>
where
	Block: BlockT,
	Net: NetworkRequest + ?Sized,
{
	let peers = match sync_service.peers_info().await {
		Ok(peers) => peers.into_iter().map(|(peer, _)| peer).collect::<Vec<_>>(),
		Err(_) => {
			log::warn!(target: LOG_TARGET, "peers_info() channel cancelled");
			return None;
		},
	};
	if peers.is_empty() {
		log::debug!(
			target: LOG_TARGET,
			"no connected sync peers, cannot fetch {:?} via bitswap yet",
			content_hash,
		);
		return None;
	}

	let client = BitswapClient::new();
	for peer in peers.into_iter().take(MAX_PEERS_PER_HASH) {
		let fut = client.fetch(network, peer, content_hash);
		let timed = with_timeout(fut, BITSWAP_PER_PEER_TIMEOUT).await;
		match timed {
			Some(Ok(Some(data))) => {
				log::debug!(
					target: LOG_TARGET,
					"bitswap fetch {:?} from {peer:?}: got {} bytes",
					content_hash,
					data.len(),
				);
				return Some(data);
			},
			Some(Ok(None)) => {},
			Some(Err(BitswapError::HashMismatch)) => {
				log::warn!(
					target: LOG_TARGET,
					"bitswap fetch {:?} from {peer:?}: hash mismatch",
					content_hash,
				);
			},
			Some(Err(e)) => {
				log::debug!(
					target: LOG_TARGET,
					"bitswap fetch {:?} from {peer:?}: {e:?}",
					content_hash,
				);
			},
			None => {
				log::debug!(
					target: LOG_TARGET,
					"bitswap fetch {:?} from {peer:?}: timeout",
					content_hash,
				);
			},
		}
	}
	None
}

async fn with_timeout<F, T>(fut: F, timeout: Duration) -> Option<T>
where
	F: core::future::Future<Output = T>,
{
	futures::select! {
		res = fut.fuse() => Some(res),
		_ = futures_timer::Delay::new(timeout).fuse() => None,
	}
}

#[cfg(test)]
mod tests {
	use super::{
		super::indexed_transactions_api::{HashingAlgorithm, IndexedTransactionInfo},
		*,
	};

	fn info(h: [u8; 32]) -> IndexedTransactionInfo {
		IndexedTransactionInfo {
			content_hash: h,
			size: 0,
			hashing: HashingAlgorithm::Blake2b256,
			cid_codec: 0x55,
		}
	}

	#[test]
	fn is_supported_accepts_blake2_raw() {
		assert!(is_supported(&info([0u8; 32])));
	}

	#[test]
	fn is_supported_rejects_sha2() {
		let mut i = info([0u8; 32]);
		i.hashing = HashingAlgorithm::Sha2_256;
		assert!(!is_supported(&i));
	}

	#[test]
	fn is_supported_rejects_keccak() {
		let mut i = info([0u8; 32]);
		i.hashing = HashingAlgorithm::Keccak256;
		assert!(!is_supported(&i));
	}

	#[test]
	fn is_supported_rejects_dag_pb() {
		let mut i = info([0u8; 32]);
		i.cid_codec = 0x70;
		assert!(!is_supported(&i));
	}

	#[tokio::test]
	async fn with_timeout_fires() {
		let fut = async {
			futures_timer::Delay::new(Duration::from_millis(200)).await;
			42u32
		};
		assert!(with_timeout(fut, Duration::from_millis(10)).await.is_none());
	}

	#[tokio::test]
	async fn with_timeout_returns_value() {
		let fut = async { 7u32 };
		assert_eq!(with_timeout(fut, Duration::from_millis(100)).await, Some(7));
	}
}
