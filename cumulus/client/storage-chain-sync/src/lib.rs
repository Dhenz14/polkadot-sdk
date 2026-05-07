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

//! `BlockImport` wrapper that ferries missing TRANSACTION-column entries to the inner import.
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
//!    data is verified against the algorithm declared by the runtime.
//! 4. The verified `(content_hash, bytes)` pairs are attached to `BlockImportParams.intermediates`
//!    under [`PREFETCHED_INDEXED_TRANSACTIONS_INTERMEDIATE_KEY`]. The inner client extracts the
//!    payload and forwards it to the backend, which writes the data to the TRANSACTION column in
//!    the **same atomic commit** as the block's BODY_INDEX entries. The inner
//!    `apply_index_ops::Renew` path's `transaction.reference()` then balances against the
//!    per-occurrence prune-time `transaction.release()`.
//!
//! Behaviour gated by `BlockOrigin`: blocks from `WarpSync`, `GapSync`, `File`, and `Genesis` are
//! passed through unchanged. Gap-sync coverage is a separate workstream.

mod fetcher;

pub use fetcher::{FetchError, IndexedTransactionFetcher, NetworkHandle, SyncingHandle};

use futures::stream::{StreamExt, TryStreamExt};
use sc_client_api::backend::{
	Backend as BackendT, PREFETCHED_INDEXED_TRANSACTIONS_INTERMEDIATE_KEY,
};
use sc_client_db::{
	classify_indexed_extrinsics, Backend, ClassifiedExtrinsic, IndexedTransactionMeta,
};
use sc_consensus::{BlockCheckParams, BlockImport, BlockImportParams, ImportResult};
use sp_api::{ApiExt, ProvideRuntimeApi};
use sp_blockchain::Backend as BlockchainBackendT;
use sp_consensus::{BlockOrigin, Error as ConsensusError};
use sp_runtime::traits::{Block as BlockT, Header as HeaderT};
use sp_transaction_storage_proof::{
	runtime_api::TransactionStorageApi, HashingAlgorithm, IndexedTransactionInfo,
};
use std::{collections::HashSet, marker::PhantomData, sync::Arc};

const LOG_TARGET: &str = "storage-chain-block-import";
const RAW_CID_CODEC: u64 = 0x55;
/// Maximum number of bitswap fetches that run concurrently for a single block. The cap exists so
/// that a bulk-renew block (e.g. `process_auto_renewals` with hundreds of hashes) cannot saturate
/// the substrate request-response queue, which has its own per-peer bound.
const MAX_CONCURRENT_RENEW_FETCHES: usize = 8;

/// Block-import wrapper that bitswap-fetches missing TRANSACTION-column entries
/// for tip-sync blocks before delegating to the inner block import.
pub struct StorageChainBlockImport<Block: BlockT, Inner, Client> {
	inner: Inner,
	client: Arc<Client>,
	backend: Arc<Backend<Block>>,
	fetcher: IndexedTransactionFetcher<Block>,
	_phantom: PhantomData<Block>,
}

impl<Block: BlockT, Inner: Clone, Client> Clone for StorageChainBlockImport<Block, Inner, Client> {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
			client: self.client.clone(),
			backend: self.backend.clone(),
			fetcher: self.fetcher.clone(),
			_phantom: PhantomData,
		}
	}
}

impl<Block: BlockT, Inner, Client> StorageChainBlockImport<Block, Inner, Client> {
	pub fn new(
		inner: Inner,
		client: Arc<Client>,
		backend: Arc<Backend<Block>>,
		fetcher: IndexedTransactionFetcher<Block>,
	) -> Self {
		Self { inner, client, backend, fetcher, _phantom: PhantomData }
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
		mut params: BlockImportParams<Block>,
	) -> Result<ImportResult, Self::Error> {
		let renews = self.classify_renew_hashes(&params)?;
		let missing = self.filter_missing(renews);
		let fetched = self.fetch_all(missing).await?;
		Self::attach_prefetched(&mut params, fetched)?;
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

	/// Returns every renew (hash, hashing) pair declared by the runtime for this block. Pure —
	/// does no DB lookup; [`Self::filter_missing`] filters this set down to entries whose data is
	/// not yet on disk.
	///
	/// `indexed_transactions(block_n)` is contracted (per `TransactionStorageApi v2`) to return an
	/// empty vec for blocks outside the runtime's retention window — so calls for blocks far below
	/// the tip silently yield an empty set, which is the right behaviour for a tip-only wrapper.
	fn classify_renew_hashes(
		&self,
		params: &BlockImportParams<Block>,
	) -> Result<HashSet<([u8; 32], HashingAlgorithm)>, ConsensusError> {
		if !self.should_intercept(params) {
			return Ok(HashSet::new());
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

		let body = params.body.as_ref().ok_or_else(|| {
			ConsensusError::Other("StorageChainBlockImport: body absent after gate".into())
		})?;

		let renews = body_classify_renews::<Block>(&infos, body);

		if !renews.is_empty() {
			log::debug!(
				target: LOG_TARGET,
				"block #{:?} ({:?}): {} indexed entries, {} renew hashes",
				block_number,
				parent_hash,
				infos.len(),
				renews.len(),
			);
		}

		Ok(renews)
	}

	/// Drops every entry whose data is already in the local TRANSACTION column.
	fn filter_missing(
		&self,
		renews: HashSet<([u8; 32], HashingAlgorithm)>,
	) -> HashSet<([u8; 32], HashingAlgorithm)> {
		renews
			.into_iter()
			.filter(|(hash, _)| {
				!self
					.backend
					.blockchain()
					.has_indexed_transaction((*hash).into())
					.unwrap_or(false)
			})
			.collect()
	}

	/// Resolves every missing entry concurrently (capped at [`MAX_CONCURRENT_RENEW_FETCHES`]),
	/// holding the fetched bytes in memory. Returns `Err` on the first failure, abandoning any
	/// in-flight fetches; their network requests time out naturally.
	async fn fetch_all(
		&self,
		missing: HashSet<([u8; 32], HashingAlgorithm)>,
	) -> Result<Vec<([u8; 32], HashingAlgorithm, Vec<u8>)>, ConsensusError> {
		futures::stream::iter(missing)
			.map(|(hash, hashing)| async move {
				let data = self.fetcher.fetch(hash, hashing).await.map_err(|e| {
					ConsensusError::Other(
						format!("bitswap fetch for {hash:?}: {e}").into(),
					)
				})?;
				Ok::<_, ConsensusError>((hash, hashing, data))
			})
			.buffer_unordered(MAX_CONCURRENT_RENEW_FETCHES)
			.try_collect()
			.await
	}

	/// Verifies every fetched blob against its declared content hash and attaches the resulting
	/// `Vec<([u8; 32], Vec<u8>)>` to `params.intermediates` under
	/// [`PREFETCHED_INDEXED_TRANSACTIONS_INTERMEDIATE_KEY`]. The inner client extracts the
	/// payload in `apply_block` and forwards it to the backend, which stores the bytes in the
	/// TRANSACTION column atomically with the block's BODY_INDEX writes.
	///
	/// No-op when `fetched` is empty so we don't pollute the intermediates map.
	fn attach_prefetched(
		params: &mut BlockImportParams<Block>,
		fetched: Vec<([u8; 32], HashingAlgorithm, Vec<u8>)>,
	) -> Result<(), ConsensusError> {
		if fetched.is_empty() {
			return Ok(());
		}
		let mut payload: Vec<([u8; 32], Vec<u8>)> = Vec::with_capacity(fetched.len());
		for (hash, hashing, data) in fetched {
			let computed = hashing.hash(&data);
			if computed != hash {
				return Err(ConsensusError::Other(
					format!(
						"prefetched indexed transaction hash mismatch: declared={hash:?}, \
						 computed={computed:?}"
					)
					.into(),
				));
			}
			log::info!(
				target: LOG_TARGET,
				"attaching bitswap-fetched indexed transaction {:?} to BlockImportParams",
				hash,
			);
			payload.push((hash, data));
		}
		params.insert_intermediate(PREFETCHED_INDEXED_TRANSACTIONS_INTERMEDIATE_KEY, payload);
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

/// Pure: given runtime-provided indexed-transaction metadata and a block body, returns the set of
/// renew (hash, hashing) pairs whose data is **not** carried in the body — i.e. the entries the
/// caller needs to fetch from elsewhere.
///
/// Has no side effects (no DB, no network, no `&self`). Filters out entries whose `cid_codec` is
/// not the IPFS RAW codec (these are not bitswap-fetchable). Multi-renew shapes (multiple metas
/// at the same `extrinsic_index`) are flattened into individual hashes.
fn body_classify_renews<Block: BlockT>(
	infos: &[IndexedTransactionInfo],
	body: &[Block::Extrinsic],
) -> HashSet<([u8; 32], HashingAlgorithm)> {
	let db_meta: Vec<IndexedTransactionMeta> =
		infos.iter().filter(is_supported).map(to_db_meta).collect();

	if db_meta.is_empty() {
		return HashSet::new();
	}

	classify_indexed_extrinsics::<Block>(body, &db_meta)
		.into_iter()
		.filter_map(|entry| match entry {
			ClassifiedExtrinsic::Renew { hashes } => Some(hashes),
			_ => None,
		})
		.flatten()
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;
	use codec::Encode;
	use sp_runtime::{generic, traits::BlakeTwo256, OpaqueExtrinsic};
	use std::collections::HashSet;

	type Block = generic::Block<generic::Header<u32, BlakeTwo256>, OpaqueExtrinsic>;

	fn info(
		content_hash: [u8; 32],
		size: u32,
		alg: HashingAlgorithm,
		codec: u64,
		extrinsic_index: u32,
	) -> IndexedTransactionInfo {
		IndexedTransactionInfo {
			content_hash,
			size,
			hashing: alg,
			cid_codec: codec,
			extrinsic_index,
		}
	}

	fn extrinsic(bytes: &[u8]) -> OpaqueExtrinsic {
		OpaqueExtrinsic::from_blob(bytes.to_vec())
	}

	fn body_info(
		ext: &OpaqueExtrinsic,
		extrinsic_index: u32,
		hashing: HashingAlgorithm,
		codec: u64,
	) -> IndexedTransactionInfo {
		let encoded = ext.encode();
		info(hashing.hash(&encoded), encoded.len() as u32, hashing, codec, extrinsic_index)
	}

	#[test]
	fn is_supported_accepts_all_hashings_with_raw_codec() {
		for algo in [
			HashingAlgorithm::Blake2b256,
			HashingAlgorithm::Sha2_256,
			HashingAlgorithm::Keccak256,
		] {
			let i = info([0u8; 32], 100, algo, RAW_CID_CODEC, u32::MAX);
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
			let i = info([0u8; 32], 100, algo, 0x70, u32::MAX);
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
	fn body_classify_renews_returns_empty_for_supported_insert() {
		let body = vec![extrinsic(&[1, 2, 3])];
		let infos = vec![body_info(&body[0], 0, HashingAlgorithm::Blake2b256, RAW_CID_CODEC)];

		assert!(body_classify_renews::<Block>(&infos, &body).is_empty());
	}

	#[test]
	fn body_classify_renews_filters_unsupported_non_raw_codec() {
		let body = vec![extrinsic(&[4, 5, 6])];
		let infos = vec![info(
			[9; 32],
			body[0].encode().len() as u32,
			HashingAlgorithm::Blake2b256,
			0x70,
			0,
		)];

		assert!(body_classify_renews::<Block>(&infos, &body).is_empty());
	}

	#[test]
	fn body_classify_renews_returns_single_supported_renew() {
		let body = vec![extrinsic(&[7, 8, 9])];
		let infos = vec![info(
			[1; 32],
			body[0].encode().len() as u32,
			HashingAlgorithm::Sha2_256,
			RAW_CID_CODEC,
			0,
		)];

		let renews = body_classify_renews::<Block>(&infos, &body);
		assert_eq!(renews, HashSet::from([([1; 32], HashingAlgorithm::Sha2_256)]));
	}

	#[test]
	fn body_classify_renews_flattens_multi_renews_at_same_index() {
		let body = vec![extrinsic(&[10, 11, 12])];
		let encoded_len = body[0].encode().len() as u32;
		let infos = vec![
			info([2; 32], encoded_len, HashingAlgorithm::Blake2b256, RAW_CID_CODEC, 0),
			info([3; 32], encoded_len, HashingAlgorithm::Keccak256, RAW_CID_CODEC, 0),
		];

		let renews = body_classify_renews::<Block>(&infos, &body);
		assert_eq!(
			renews,
			HashSet::from([
				([2; 32], HashingAlgorithm::Blake2b256),
				([3; 32], HashingAlgorithm::Keccak256),
			]),
		);
	}
}
