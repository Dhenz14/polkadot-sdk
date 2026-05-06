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

//! `BlockImport` wrapper that fills missing TRANSACTION-column entries before delegating to the
//! inner import.
//!
//! Storage-chain parachains track indexed-data refcounts in `sc-client-db`'s TRANSACTION column.
//! When a tip block contains a `Renew` operation for indexed data the local node has never
//! stored, the inner block import calls `transaction.reference()` on a missing key — a silent
//! no-op in kvdb — and the refcount stays at zero. Subsequent prune cycles then drop entries the
//! runtime considered live.
//!
//! `StorageChainBlockImport` interposes between consensus and the inner import:
//!
//! 1. On every incoming tip block (`NetworkInitialSync` / `NetworkBroadcast` /
//!    `ConsensusBroadcast` / `Own` origin, `body.is_some()`, runtime exposes
//!    `TransactionStorageApi >= 2`), it asks the runtime which indexed transactions the block
//!    references via `indexed_transactions(block_number)`.
//! 2. It feeds the body and runtime metadata through `sc_client_db::classify_indexed_extrinsics`,
//!    which returns the renew hashes whose data is **not** carried in the body and is **not**
//!    already on disk.
//! 3. For each such hash it issues a bitswap `WANT-BLOCK` to a connected peer. On success the
//!    data is verified using the algorithm declared by the runtime and stored via
//!    `Backend::store_fetched_transaction_with_count(.., target_ref_count = 1)`.
//! 4. It then delegates to the inner block import. The inner `apply_index_ops::Renew` path now
//!    finds an existing TRANSACTION entry and `transaction.reference()` correctly bumps its
//!    refcount.
//!
//! Behaviour gated by `BlockOrigin`: blocks from `WarpSync`, `GapSync`, `File`, and `Genesis` are
//! passed through unchanged. Gap-sync coverage is a separate workstream.

use sc_client_api::backend::Backend as BackendT;
use sc_client_db::{
	classify_indexed_extrinsics, Backend, ClassifiedExtrinsic, IndexedTransactionMeta,
};
use sc_consensus::{BlockCheckParams, BlockImport, BlockImportParams, ImportResult};
use sc_network::{
	bitswap::{BitswapClient, BitswapError},
	NetworkRequest,
};
use sc_network_sync::SyncingService;
use sp_api::{ApiExt, ProvideRuntimeApi};
use sp_blockchain::Backend as BlockchainBackendT;
use sp_consensus::{BlockOrigin, Error as ConsensusError};
use sp_runtime::traits::{Block as BlockT, Header as HeaderT};
use sp_transaction_storage_proof::{
	runtime_api::TransactionStorageApi, HashingAlgorithm, IndexedTransactionInfo,
};
use std::{
	collections::HashSet,
	marker::PhantomData,
	sync::{Arc, Mutex, OnceLock},
	time::Duration,
};

const LOG_TARGET: &str = "storage-chain-block-import";
const RAW_CID_CODEC: u64 = 0x55;
const BITSWAP_PER_PEER_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PEERS_PER_HASH: usize = 8;

/// Late-bound network handles populated after `build_network` returns.
pub type NetworkHandle = Arc<OnceLock<Arc<dyn NetworkRequest + Send + Sync>>>;
/// Late-bound `SyncingService` handle populated after `build_network` returns.
pub type SyncingHandle<Block> = Arc<OnceLock<Arc<SyncingService<Block>>>>;

/// Block-import wrapper that bitswap-fetches missing TRANSACTION-column entries
/// for tip-sync blocks before delegating to the inner block import.
pub struct StorageChainBlockImport<Block: BlockT, Inner, Client> {
	inner: Inner,
	client: Arc<Client>,
	backend: Arc<Backend<Block>>,
	network: NetworkHandle,
	syncing_service: SyncingHandle<Block>,
	inflight: Arc<Mutex<HashSet<[u8; 32]>>>,
	_phantom: PhantomData<Block>,
}

impl<Block: BlockT, Inner: Clone, Client> Clone for StorageChainBlockImport<Block, Inner, Client> {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
			client: self.client.clone(),
			backend: self.backend.clone(),
			network: self.network.clone(),
			syncing_service: self.syncing_service.clone(),
			inflight: self.inflight.clone(),
			_phantom: PhantomData,
		}
	}
}

impl<Block: BlockT, Inner, Client> StorageChainBlockImport<Block, Inner, Client> {
	pub fn new(
		inner: Inner,
		client: Arc<Client>,
		backend: Arc<Backend<Block>>,
		network: NetworkHandle,
		syncing_service: SyncingHandle<Block>,
	) -> Self {
		Self {
			inner,
			client,
			backend,
			network,
			syncing_service,
			inflight: Arc::new(Mutex::new(HashSet::new())),
			_phantom: PhantomData,
		}
	}
}

#[async_trait::async_trait]
impl<Block, Inner, Client> BlockImport<Block> for StorageChainBlockImport<Block, Inner, Client>
where
	Block: BlockT<Hash = sc_client_db::DbHash>,
	Inner: BlockImport<Block, Error = ConsensusError> + Send + Sync,
	Client: ProvideRuntimeApi<Block> + Send + Sync,
	Client::Api: TransactionStorageApi<Block>,
{
	type Error = ConsensusError;

	async fn check_block(
		&self,
		block: BlockCheckParams<Block>,
	) -> Result<ImportResult, Self::Error> {
		self.inner.check_block(block).await
	}

	async fn import_block(
		&self,
		params: BlockImportParams<Block>,
	) -> Result<ImportResult, Self::Error> {
		let to_fetch = self.classify_missing_renews(&params)?;
		for (content_hash, hashing) in to_fetch {
			self.fetch_and_store_one(content_hash, hashing).await?;
		}
		self.inner.import_block(params).await
	}
}

impl<Block, Inner, Client> StorageChainBlockImport<Block, Inner, Client>
where
	Block: BlockT<Hash = sc_client_db::DbHash>,
	Client: ProvideRuntimeApi<Block> + Send + Sync,
	Client::Api: TransactionStorageApi<Block>,
{
	/// Returns `true` iff the block should pass through the bitswap-fetch path.
	///
	/// The wrapper acts only on tip-sync blocks for runtimes that expose
	/// `TransactionStorageApi >= 2`. Warp-sync and gap-sync blocks (origin `WarpSync` /
	/// `GapSync`) are passed straight through; their TRANSACTION-column population is the
	/// responsibility of separate workstreams.
	fn should_intercept(&self, params: &BlockImportParams<Block>) -> bool {
		if params.body.is_none() {
			return false;
		}
		match params.origin {
			BlockOrigin::NetworkInitialSync
			| BlockOrigin::NetworkBroadcast
			| BlockOrigin::ConsensusBroadcast
			| BlockOrigin::Own => {},
			BlockOrigin::Genesis
			| BlockOrigin::File
			| BlockOrigin::WarpSync
			| BlockOrigin::GapSync => return false,
		}
		let parent_hash = *params.header.parent_hash();
		self.client
			.runtime_api()
			.has_api_with::<dyn TransactionStorageApi<Block>, _>(parent_hash, |v| v >= 2)
			.unwrap_or(false)
	}

	/// Determine which renew hashes are missing locally for this block.
	///
	/// `indexed_transactions(block_n)` is contracted (per `TransactionStorageApi v2`) to return an
	/// empty vec for blocks outside the runtime's retention window — so calls for blocks far below
	/// the tip silently yield an empty fetch set, which is the right behaviour for a tip-only
	/// wrapper.
	fn classify_missing_renews(
		&self,
		params: &BlockImportParams<Block>,
	) -> Result<Vec<([u8; 32], HashingAlgorithm)>, ConsensusError> {
		if !self.should_intercept(params) {
			return Ok(Vec::new());
		}

		let parent_hash = *params.header.parent_hash();
		let block_number = *params.header.number();

		let infos = self
			.client
			.runtime_api()
			.indexed_transactions(parent_hash, block_number)
			.map_err(|e| {
				ConsensusError::Other(
					format!("indexed_transactions runtime API failed: {e}").into(),
				)
			})?;

		if infos.is_empty() {
			return Ok(Vec::new());
		}

		let db_meta: Vec<IndexedTransactionMeta> =
			infos.iter().filter(is_supported).map(to_db_meta).collect();

		if db_meta.is_empty() {
			return Ok(Vec::new());
		}

		let body = params.body.as_ref().ok_or_else(|| {
			ConsensusError::Other("StorageChainBlockImport: body absent after gate".into())
		})?;

		let classified = classify_indexed_extrinsics::<Block>(body, &db_meta);
		let mut seen = HashSet::new();
		let mut missing: Vec<([u8; 32], HashingAlgorithm)> = Vec::new();
		for entry in classified {
			let ClassifiedExtrinsic::Renew { hashes } = entry else {
				continue;
			};
			for (hash, hashing) in hashes {
				let mut bytes = [0u8; 32];
				bytes.copy_from_slice(hash.as_ref());
				if seen.insert(bytes) {
					missing.push((bytes, hashing));
				}
			}
		}

		if !missing.is_empty() {
			log::debug!(
				target: LOG_TARGET,
				"block #{:?} ({:?}): {} indexed entries, {} missing-renew hashes to fetch",
				block_number,
				parent_hash,
				db_meta.len(),
				missing.len(),
			);
		}

		Ok(missing)
	}

	async fn fetch_and_store_one(
		&self,
		content_hash: [u8; 32],
		hashing: HashingAlgorithm,
	) -> Result<(), ConsensusError> {
		if self
			.backend
			.blockchain()
			.has_indexed_transaction(content_hash.into())
			.unwrap_or(false)
		{
			return Ok(());
		}

		let claimed = {
			let mut guard = self
				.inflight
				.lock()
				.map_err(|_| ConsensusError::Other("inflight mutex poisoned".into()))?;
			guard.insert(content_hash)
		};
		if !claimed {
			return Ok(());
		}

		let result = self.do_fetch_and_store(content_hash, hashing).await;

		if let Ok(mut guard) = self.inflight.lock() {
			guard.remove(&content_hash);
		}

		result
	}

	async fn do_fetch_and_store(
		&self,
		content_hash: [u8; 32],
		hashing: HashingAlgorithm,
	) -> Result<(), ConsensusError> {
		let network = self.network.get().ok_or_else(|| {
			ConsensusError::Other(
				"StorageChainBlockImport: network handle not yet set; \
				 storage-chain blocks cannot be imported before build_network completes"
					.into(),
			)
		})?;
		let sync = self.syncing_service.get().ok_or_else(|| {
			ConsensusError::Other(
				"StorageChainBlockImport: sync handle not yet set; \
				 storage-chain blocks cannot be imported before build_network completes"
					.into(),
			)
		})?;

		let data =
			fetch_via_bitswap::<Block>(network.as_ref(), sync.as_ref(), content_hash, hashing)
				.await
				.ok_or_else(|| {
					ConsensusError::Other(
						format!(
							"bitswap fetch failed for indexed transaction {content_hash:?}; \
							 retry block import after peers respond"
						)
						.into(),
					)
				})?;

		if self
			.backend
			.blockchain()
			.has_indexed_transaction(content_hash.into())
			.unwrap_or(false)
		{
			return Ok(());
		}

		self.backend
			.store_fetched_transaction_with_count(content_hash, data, 1, hashing)
			.map_err(|e| {
				ConsensusError::Other(
					format!(
						"store_fetched_transaction_with_count({content_hash:?}) failed: {e}"
					)
					.into(),
				)
			})?;

		log::info!(
			target: LOG_TARGET,
			"bitswap-fetched indexed transaction {:?} (rc=1) ahead of inner import",
			content_hash,
		);

		Ok(())
	}
}

fn is_supported(info: &&IndexedTransactionInfo) -> bool {
	info.cid_codec == RAW_CID_CODEC
}

fn to_db_meta(info: &IndexedTransactionInfo) -> IndexedTransactionMeta {
	IndexedTransactionMeta {
		content_hash: info.content_hash,
		size: info.size,
		extrinsic_index: info.extrinsic_index,
		hashing: info.hashing,
	}
}

async fn fetch_via_bitswap<Block: BlockT>(
	network: &(dyn NetworkRequest + Send + Sync),
	sync: &SyncingService<Block>,
	content_hash: [u8; 32],
	hashing: HashingAlgorithm,
) -> Option<Vec<u8>> {
	let peers = match sync.peers_info().await {
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
		let fut = client.fetch(network, peer, content_hash, hashing);
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
	F: std::future::Future<Output = T>,
{
	use futures::FutureExt;
	futures::select! {
		v = fut.fuse() => Some(v),
		_ = futures_timer::Delay::new(timeout).fuse() => None,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn info(
		content_hash: [u8; 32],
		size: u32,
		alg: HashingAlgorithm,
		codec: u64,
	) -> IndexedTransactionInfo {
		IndexedTransactionInfo {
			content_hash,
			size,
			hashing: alg,
			cid_codec: codec,
			extrinsic_index: u32::MAX,
		}
	}

	#[test]
	fn is_supported_accepts_all_hashings_with_raw_codec() {
		for algo in [
			HashingAlgorithm::Blake2b256,
			HashingAlgorithm::Sha2_256,
			HashingAlgorithm::Keccak256,
		] {
			let i = info([0u8; 32], 100, algo, RAW_CID_CODEC);
			assert!(is_supported(&&i), "{algo:?} should be supported with RAW codec");
		}
	}

	#[test]
	fn is_supported_rejects_non_raw_codec() {
		for algo in [
			HashingAlgorithm::Blake2b256,
			HashingAlgorithm::Sha2_256,
			HashingAlgorithm::Keccak256,
		] {
			let i = info([0u8; 32], 100, algo, 0x70);
			assert!(!is_supported(&&i), "{algo:?} with non-RAW codec should be rejected");
		}
	}

	#[test]
	fn to_db_meta_preserves_all_fields() {
		let h = [7u8; 32];
		let i = IndexedTransactionInfo {
			content_hash: h,
			size: 4096,
			hashing: HashingAlgorithm::Sha2_256,
			cid_codec: RAW_CID_CODEC,
			extrinsic_index: 17,
		};
		let meta = to_db_meta(&i);
		assert_eq!(meta.content_hash, h);
		assert_eq!(meta.size, 4096);
		assert_eq!(meta.extrinsic_index, 17);
		assert_eq!(meta.hashing, HashingAlgorithm::Sha2_256);
	}

	#[test]
	fn raw_cid_codec_matches_upstream_bitswap_constant() {
		assert_eq!(RAW_CID_CODEC, 0x55);
	}

	#[test]
	fn intercept_origins_contain_only_live_origins() {
		fn allowed(o: BlockOrigin) -> bool {
			matches!(
				o,
				BlockOrigin::NetworkInitialSync
					| BlockOrigin::NetworkBroadcast
					| BlockOrigin::ConsensusBroadcast
					| BlockOrigin::Own,
			)
		}
		assert!(allowed(BlockOrigin::NetworkInitialSync));
		assert!(allowed(BlockOrigin::NetworkBroadcast));
		assert!(allowed(BlockOrigin::ConsensusBroadcast));
		assert!(allowed(BlockOrigin::Own));
		assert!(!allowed(BlockOrigin::Genesis));
		assert!(!allowed(BlockOrigin::File));
		assert!(!allowed(BlockOrigin::WarpSync));
		assert!(!allowed(BlockOrigin::GapSync));
	}
}
