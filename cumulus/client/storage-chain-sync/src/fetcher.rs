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
//! [`IndexedTransactionFetcher::fetch`] for a given `(content_hash, hashing)` pair and what to do
//! with the returned bytes.

use sc_network::{
	bitswap::{BitswapClient, BitswapError},
	NetworkRequest,
};
use sc_network_sync::SyncingService;
use sp_runtime::traits::Block as BlockT;
use sp_transaction_storage_proof::HashingAlgorithm;
use std::{
	sync::{Arc, OnceLock},
	time::Duration,
};

const LOG_TARGET: &str = "storage-chain-fetcher";
const BITSWAP_PER_PEER_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PEERS_PER_HASH: usize = 8;

/// Late-bound network handles populated after `build_network` returns.
pub type NetworkHandle = Arc<OnceLock<Arc<dyn NetworkRequest + Send + Sync>>>;
/// Late-bound `SyncingService` handle populated after `build_network` returns.
pub type SyncingHandle<Block> = Arc<OnceLock<Arc<SyncingService<Block>>>>;

/// Reasons an [`IndexedTransactionFetcher::fetch`] call can fail.
#[derive(Debug)]
pub enum FetchError {
	/// The late-bound network handle has not been populated yet (i.e. `build_network` has not
	/// returned).
	NetworkHandleUnset,
	/// The late-bound `SyncingService` handle has not been populated yet.
	SyncingHandleUnset,
	/// No connected sync peers, or every candidate peer failed to serve the data.
	NotFound,
}

impl std::fmt::Display for FetchError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::NetworkHandleUnset =>
				f.write_str("network handle not yet set; storage-chain blocks cannot be fetched before build_network completes"),
			Self::SyncingHandleUnset =>
				f.write_str("sync handle not yet set; storage-chain blocks cannot be fetched before build_network completes"),
			Self::NotFound =>
				f.write_str("no peer served the requested indexed transaction"),
		}
	}
}

impl std::error::Error for FetchError {}

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

	/// Resolve a single hash via bitswap. Pure fetch — no DB write, no verification beyond the
	/// transport-level check that the returned bytes hash to `content_hash`. Caller decides what
	/// to do with the bytes.
	///
	/// Returns [`FetchError::NotFound`] when no connected peer was able to serve the data within
	/// the per-peer timeout (no retry across calls; that's the caller's choice today).
	pub async fn fetch(
		&self,
		content_hash: [u8; 32],
		hashing: HashingAlgorithm,
	) -> Result<Vec<u8>, FetchError> {
		let network = self.network.get().ok_or(FetchError::NetworkHandleUnset)?;
		let sync = self.syncing_service.get().ok_or(FetchError::SyncingHandleUnset)?;

		fetch_via_bitswap::<Block>(network.as_ref(), sync.as_ref(), content_hash, hashing)
			.await
			.ok_or(FetchError::NotFound)
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
