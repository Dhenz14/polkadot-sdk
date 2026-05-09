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

use async_trait::async_trait;
use futures::channel::oneshot;
use sc_network::{
	bitswap::{self, BitswapRequestSender, FetchOutcome, MAX_WANTED_BLOCKS_PER_REQUEST},
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

/// Source of currently-connected sync peer IDs. Abstracted so the fetcher can be unit-tested
/// without spinning up a full `SyncingService`. The production blanket impl on
/// `SyncingService<Block>` calls `peers_info()` and projects to the peer-id column.
#[async_trait]
pub trait BitswapPeerSource: Send + Sync {
	async fn current_peers(&self) -> Result<Vec<PeerId>, oneshot::Canceled>;
}

#[async_trait]
impl<B: BlockT> BitswapPeerSource for SyncingService<B> {
	async fn current_peers(&self) -> Result<Vec<PeerId>, oneshot::Canceled> {
		Ok(self.peers_info().await?.into_iter().map(|(peer, _)| peer).collect())
	}
}

/// Late-bound network handle populated after `build_network` returns.
pub type NetworkHandle = Arc<OnceLock<Arc<dyn NetworkRequest + Send + Sync>>>;
/// Late-bound peer-source handle populated after `build_network` returns. Production code coerces
/// `Arc<SyncingService<Block>>` to `Arc<dyn BitswapPeerSource + Send + Sync>` via the blanket impl
/// in this module.
pub type SyncingHandle = Arc<OnceLock<Arc<dyn BitswapPeerSource + Send + Sync>>>;

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

/// Fetcher that resolves indexed-transaction hashes via bitswap.
///
/// Owns the late-bound network/sync handles plus the per-peer iteration policy. The block-import
/// path holds one of these and calls [`Self::fetch_many`] (verified path, runtime-API discovery)
/// or [`Self::fetch_many_unverified`] (unverified path, host-call discovery) for each batch of
/// missing renew hashes.
///
/// Cloning is cheap: every field is an `Arc`-equivalent.
pub struct IndexedTransactionFetcher<Block: BlockT> {
	network: NetworkHandle,
	peer_source: SyncingHandle,
	_phantom: std::marker::PhantomData<Block>,
}

impl<Block: BlockT> Clone for IndexedTransactionFetcher<Block> {
	fn clone(&self) -> Self {
		Self {
			network: self.network.clone(),
			peer_source: self.peer_source.clone(),
			_phantom: std::marker::PhantomData,
		}
	}
}

impl<Block: BlockT> IndexedTransactionFetcher<Block> {
	/// Build a new fetcher backed by the given late-bound handles.
	pub fn new(network: NetworkHandle, peer_source: SyncingHandle) -> Self {
		Self { network, peer_source, _phantom: std::marker::PhantomData }
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
		let peer_source = self.peer_source.get().ok_or(FetchError::SyncingHandleUnset)?;

		let peers = match peer_source.current_peers().await {
			Ok(peers) => peers,
			Err(_) => {
				log::warn!(target: LOG_TARGET, "current_peers() channel cancelled");
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

		for peer in peers.into_iter().take(MAX_PEERS_PER_IMPORT) {
			if remaining.is_empty() {
				break;
			}
			let from_peer = try_fetch_from_peer(network.as_ref(), peer, &remaining).await;
			acquired.extend(from_peer);
			remaining.retain(|(hash, _)| !acquired.contains_key(hash));
		}

		Ok(acquired)
	}

	/// Resolve a batch of `ContentHash`es via bitswap across up to [`MAX_PEERS_PER_IMPORT`] peers.
	///
	/// Differs from [`Self::fetch_many`] in that the caller does NOT supply a `HashingAlgorithm`
	/// per hash. This is for renews discovered via `IndexOperation::Renew { hash, .. }`
	/// host-call output, which carries only the 32-byte content hash. The substrate bitswap
	/// server is algorithm-agnostic (looks up by 32-byte digest only) so the request succeeds
	/// regardless of the real hashing algorithm; the caller must verify integrity by other means
	/// (post-commit runtime-API cross-check).
	///
	/// Sends one WANT-BLOCK per hash per peer (the unverified bitswap path is single-WANT).
	/// Returns only successfully fetched entries.
	pub async fn fetch_many_unverified(
		&self,
		wants: &[ContentHash],
	) -> Result<HashMap<ContentHash, Vec<u8>>, FetchError> {
		if wants.is_empty() {
			return Ok(HashMap::new());
		}
		let network = self.network.get().ok_or(FetchError::NetworkHandleUnset)?;
		let peer_source = self.peer_source.get().ok_or(FetchError::SyncingHandleUnset)?;

		let peers = match peer_source.current_peers().await {
			Ok(peers) => peers,
			Err(_) => {
				log::warn!(target: LOG_TARGET, "current_peers() channel cancelled");
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

		let mut remaining: Vec<ContentHash> = wants.to_vec();
		let mut acquired: HashMap<ContentHash, Vec<u8>> = HashMap::new();

		for peer in peers.into_iter().take(MAX_PEERS_PER_IMPORT) {
			if remaining.is_empty() {
				break;
			}
			let from_peer =
				try_fetch_from_peer_unverified(network.as_ref(), peer, &remaining).await;
			acquired.extend(from_peer);
			remaining.retain(|hash| !acquired.contains_key(hash));
		}

		Ok(acquired)
	}
}

/// Try every chunk of `wants` against a single peer in sequence. Returns whatever blocks the
/// peer actually served. A timeout or per-chunk error aborts the remaining chunks for this peer
/// and lets the caller move on to the next one.
async fn try_fetch_from_peer<N: BitswapRequestSender + ?Sized>(
	network: &N,
	peer: PeerId,
	wants: &[(ContentHash, HashingAlgorithm)],
) -> HashMap<ContentHash, Vec<u8>> {
	let mut acquired: HashMap<ContentHash, Vec<u8>> = HashMap::new();
	for chunk in wants.chunks(MAX_WANTED_BLOCKS_PER_REQUEST) {
		match with_timeout(bitswap::fetch_many(network, peer, chunk), BITSWAP_PER_PEER_TIMEOUT)
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
			Some(Ok(per_cid)) => {
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
				}
			},
		}
	}
	acquired
}

/// Unverified-path counterpart of [`try_fetch_from_peer`]. Same chunk size as the verified path
/// because [`bitswap::fetch_many_unverified`] uses positional response correlation to handle
/// multi-WANT batches; see its docstring for the order-correlation contract.
async fn try_fetch_from_peer_unverified<N: BitswapRequestSender + ?Sized>(
	network: &N,
	peer: PeerId,
	wants: &[ContentHash],
) -> HashMap<ContentHash, Vec<u8>> {
	let mut acquired: HashMap<ContentHash, Vec<u8>> = HashMap::new();
	for chunk in wants.chunks(MAX_WANTED_BLOCKS_PER_REQUEST) {
		match with_timeout(
			bitswap::fetch_many_unverified(network, peer, chunk),
			BITSWAP_PER_PEER_TIMEOUT,
		)
		.await
		{
			None => {
				log::debug!(
					target: LOG_TARGET,
					"fetch_many_unverified to {peer:?}: timeout (chunk size {})",
					chunk.len(),
				);
				return acquired;
			},
			Some(Err(e)) => {
				log::debug!(target: LOG_TARGET, "fetch_many_unverified to {peer:?}: {e:?}");
				return acquired;
			},
			Some(Ok(per_cid)) =>
				for (hash, outcome) in per_cid {
					if let FetchOutcome::Block(data) = outcome {
						log::debug!(
							target: LOG_TARGET,
							"fetched {} unverified bytes for {:?} from {peer:?}",
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
