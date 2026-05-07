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

//! Bitswap-based fetcher for indexed transaction blobs.
//!
//! Owns the late-bound network/sync handles plus the per-peer iteration policy. Knows nothing
//! about block import: the consumer ([`crate::StorageChainBlockImport`]) decides when to call
//! [`IndexedTransactionFetcher::fetch_many`] for a batch of `(content_hash, hashing)` pairs and
//! what to do with the returned bytes.

use sc_network::{
	bitswap::{BitswapClient, FetchOutcome, MAX_WANTED_BLOCKS_PER_REQUEST},
	NetworkRequest, PeerId,
};
use sc_network_sync::SyncingService;
use sp_runtime::traits::Block as BlockT;
use sp_transaction_storage_proof::{ContentHash, HashingAlgorithm};
use std::{
	collections::HashMap,
	sync::{Arc, OnceLock},
	time::Duration,
};

const LOG_TARGET: &str = "storage-chain-fetcher";
const BITSWAP_PER_PEER_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PEERS_PER_IMPORT: usize = 8;

/// Late-bound network handles populated after `build_network` returns.
pub type NetworkHandle = Arc<OnceLock<Arc<dyn NetworkRequest + Send + Sync>>>;
/// Late-bound `SyncingService` handle populated after `build_network` returns.
pub type SyncingHandle<Block> = Arc<OnceLock<Arc<SyncingService<Block>>>>;

/// Reasons an [`IndexedTransactionFetcher::fetch_many`] call can fail outright.
///
/// Per-CID misses (peer absent, peer responded `DontHave`, peer ignored entry) are *not* errors:
/// they manifest as missing keys in the returned map. Errors here only cover infrastructure
/// preconditions that prevent any fetch from happening at all.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
	#[error("network handle not yet set; storage-chain blocks cannot be fetched before build_network completes")]
	NetworkHandleUnset,
	#[error("sync handle not yet set; storage-chain blocks cannot be fetched before build_network completes")]
	SyncingHandleUnset,
}

/// Fetcher that resolves a single indexed-transaction hash via bitswap.
///
/// Owns the late-bound network/sync handles plus the per-peer iteration policy. The block-import
/// path holds one of these and calls [`Self::fetch`] for each missing renew hash.
///
/// Cloning is cheap: every field is an `Arc`-equivalent.
pub struct IndexedTransactionFetcher<Block: BlockT> {
	network: NetworkHandle,
	syncing_service: SyncingHandle<Block>,
}

impl<Block: BlockT> Clone for IndexedTransactionFetcher<Block> {
	fn clone(&self) -> Self {
		Self {
			network: self.network.clone(),
			syncing_service: self.syncing_service.clone(),
		}
	}
}

impl<Block: BlockT> IndexedTransactionFetcher<Block> {
	/// Build a new fetcher backed by the given late-bound handles.
	pub fn new(network: NetworkHandle, syncing_service: SyncingHandle<Block>) -> Self {
		Self { network, syncing_service }
	}

	/// Resolve a batch of `(content_hash, hashing)` pairs via bitswap across up to
	/// [`MAX_PEERS_PER_IMPORT`] peers, sending one multi-entry `WANT-BLOCK` request per peer.
	///
	/// Returns only successfully fetched entries; `Missing`/`DontHave` outcomes from each peer
	/// fall through to the next peer in the candidate list. The caller detects partial fill
	/// by comparing `result.len()` against `wants.len()`.
	pub async fn fetch_many(
		&self,
		wants: &[(ContentHash, HashingAlgorithm)],
	) -> Result<HashMap<ContentHash, Vec<u8>>, FetchError> {
		if wants.is_empty() {
			return Ok(HashMap::new());
		}
		let network = self.network.get().ok_or(FetchError::NetworkHandleUnset)?;
		let sync = self.syncing_service.get().ok_or(FetchError::SyncingHandleUnset)?;

		let peers = match sync.peers_info().await {
			Ok(peers) => peers.into_iter().map(|(peer, _)| peer).collect::<Vec<_>>(),
			Err(_) => {
				log::warn!(target: LOG_TARGET, "peers_info() channel cancelled");
				return Ok(HashMap::new());
			},
		};
		if peers.is_empty() {
			log::debug!(
				target: LOG_TARGET,
				"no connected sync peers, cannot fetch via bitswap yet",
			);
			return Ok(HashMap::new());
		}

		let mut remaining: Vec<_> = wants.to_vec();
		let mut acquired: HashMap<ContentHash, Vec<u8>> = HashMap::new();
		let client = BitswapClient::new();

		for peer in peers.into_iter().take(MAX_PEERS_PER_IMPORT) {
			if remaining.is_empty() {
				break;
			}
			let from_peer =
				try_fetch_from_peer(&client, network.as_ref(), peer, &remaining).await;
			acquired.extend(from_peer);
			remaining.retain(|(hash, _)| !acquired.contains_key(hash));
		}

		Ok(acquired)
	}
}

/// Try every chunk of `wants` against a single peer in sequence. Returns whatever blocks the
/// peer actually served. A timeout or per-chunk error aborts the remaining chunks for this peer
/// and lets the caller move on to the next one.
async fn try_fetch_from_peer(
	client: &BitswapClient,
	network: &(dyn NetworkRequest + Send + Sync),
	peer: PeerId,
	wants: &[(ContentHash, HashingAlgorithm)],
) -> HashMap<ContentHash, Vec<u8>> {
	let mut acquired: HashMap<ContentHash, Vec<u8>> = HashMap::new();
	for chunk in wants.chunks(MAX_WANTED_BLOCKS_PER_REQUEST) {
		match with_timeout(
			client.fetch_many(network, peer, chunk),
			BITSWAP_PER_PEER_TIMEOUT,
		)
		.await
		{
			None => {
				log::debug!(
					target: LOG_TARGET,
					"fetch_many to {peer:?}: timeout (chunk size {})",
					chunk.len(),
				);
				return acquired;
			},
			Some(Err(e)) => {
				log::debug!(target: LOG_TARGET, "fetch_many to {peer:?}: {e:?}");
				return acquired;
			},
			Some(Ok(per_cid)) =>
				for (hash, outcome) in per_cid {
					if let FetchOutcome::Block(data) = outcome {
						log::debug!(
							target: LOG_TARGET,
							"fetched {} bytes for {:?} from {peer:?}",
							data.len(),
							hash,
						);
						acquired.insert(hash, data);
					}
				},
		}
	}
	acquired
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
