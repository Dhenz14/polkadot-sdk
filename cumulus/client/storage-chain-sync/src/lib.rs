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
//! 1. On every incoming tip block (`NetworkInitialSync` / `NetworkBroadcast` / `ConsensusBroadcast`
//!    / `Own` origin, `body.is_some()`, runtime exposes `TransactionStorageApi >= 2`), it asks the
//!    runtime which indexed transactions the block references via
//!    `indexed_transactions(block_number)`.
//! 2. It feeds the body and runtime metadata through `sc_client_db::classify_indexed_extrinsics`,
//!    which returns the renew hashes whose data is **not** carried in the body and is **not**
//!    already on disk.
//! 3. For each such hash it issues a bitswap `WANT-BLOCK` to a connected peer. On success the data
//!    is verified against the algorithm declared by the runtime.
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

pub use fetcher::{
	BitswapPeerSource, FetchError, IndexedTransactionFetcher, NetworkHandle, SyncingHandle,
};

use sc_client_api::backend::{
	Backend as BackendT, PREFETCHED_INDEXED_TRANSACTIONS_INTERMEDIATE_KEY,
};
use sc_client_db::{
	classify_indexed_extrinsics, Backend, ClassifiedExtrinsic, IndexedTransactionMeta,
};
use sc_consensus::{
	BlockCheckParams, BlockImport, BlockImportParams, ImportResult, StateAction,
	StorageChanges as ConsensusStorageChanges,
};
use sp_api::{ApiExt, CallApiAt, CallContext, Core, ProvideRuntimeApi};
use sp_blockchain::Backend as BlockchainBackendT;
use sp_consensus::{BlockOrigin, Error as ConsensusError};
use sp_runtime::traits::{Block as BlockT, HashingFor, Header as HeaderT};
use sp_state_machine::{IndexOperation, StorageChanges};
use sp_transaction_storage_proof::{
	runtime_api::TransactionStorageApi, ContentHash, HashingAlgorithm, IndexedTransactionInfo,
};
use std::{collections::HashSet, marker::PhantomData, sync::Arc};

const LOG_TARGET: &str = "storage-chain-block-import";
const RAW_CID_CODEC: u64 = 0x55;

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
	Client: ProvideRuntimeApi<Block> + CallApiAt<Block> + Send + Sync,
	Client::Api: TransactionStorageApi<Block> + Core<Block>,
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
		if !self.should_intercept(&params) {
			return self.inner.import_block(params).await;
		}

		let renews = self.classify_renew_hashes(&mut params)?;
		let missing = self.filter_missing(renews);
		let fetched = self.fetch_all(missing).await?;
		Self::attach_prefetched(&mut params, fetched);
		self.inner.import_block(params).await
	}
}

impl<Block, Inner, Client> StorageChainBlockImport<Block, Inner, Client>
where
	Block: BlockT<Hash = sc_client_db::DbHash>,
	Client: ProvideRuntimeApi<Block> + CallApiAt<Block> + Send + Sync,
	Client::Api: TransactionStorageApi<Block> + Core<Block>,
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
			BlockOrigin::NetworkInitialSync |
			BlockOrigin::NetworkBroadcast |
			BlockOrigin::ConsensusBroadcast |
			BlockOrigin::Own => {},
			BlockOrigin::Genesis |
			BlockOrigin::File |
			BlockOrigin::WarpSync |
			BlockOrigin::GapSync => return false,
		}
		let parent_hash = *params.header.parent_hash();
		self.client
			.runtime_api()
			.has_api_with::<dyn TransactionStorageApi<Block>, _>(parent_hash, |v| v >= 2)
			.unwrap_or(false)
	}

	/// Discover the renew hashes this block needs the wrapper to bitswap-fetch, dispatching
	/// across three sources depending on `params.state_action` and `params.origin`:
	///
	/// - **Case A** (already executed): `params.state_action.as_storage_changes().is_some()` —
	///   the upstream proposer (or a previous wrapper invocation) has already executed the
	///   block and populated `StorageChanges`. Read `transaction_index_changes` directly. No
	///   execution cost. Common for `BlockOrigin::Own` parachain blocks (cumulus collator
	///   pre-executes per `cumulus/client/consensus/aura/src/collator.rs:535-539`).
	///
	/// - **Case C** (gap-sync, runtime-API path): `BlockOrigin::GapSync` — block already
	///   committed against on-chain state, so `TransactionStorageApi::indexed_transactions`
	///   returns the right metadata. Currently unreachable because `should_intercept` rejects
	///   `GapSync` in PR-1; PR-2 will widen the gate. Kept here as the right discovery path
	///   for committed historical state.
	///
	/// - **Case B** (tip block, not yet executed): re-execute via [`Self::execute_block`] to
	///   obtain `transaction_index_changes`, then reassign `params.state_action` to the
	///   executed `StorageChanges` so the inner block-import takes the no-execute happy path
	///   (avoids double execution).
	///
	/// `&mut params` is required because case B mutates `params.state_action`.
	fn classify_renew_hashes(
		&self,
		params: &mut BlockImportParams<Block>,
	) -> Result<RenewHashes, ConsensusError> {
		let parent_hash = *params.header.parent_hash();
		let block_number = *params.header.number();

		if let Some(changes) = params.state_action.as_storage_changes() {
			let renews = extract_renews_from_index_ops(&changes.transaction_index_changes);
			if !renews.is_empty() {
				log::debug!(
					target: LOG_TARGET,
					"block #{block_number:?} ({parent_hash:?}): case A peek, {} renew hashes",
					renews.len(),
				);
			}
			return Ok(RenewHashes::Unverified(renews));
		}

		if matches!(params.origin, BlockOrigin::GapSync) {
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
					"block #{block_number:?} ({parent_hash:?}): case C runtime-API, \
					 {} indexed entries, {} renew hashes",
					infos.len(),
					renews.len(),
				);
			}
			return Ok(RenewHashes::Verified(renews));
		}

		let gen_storage_changes = self.execute_block(params)?;
		let renews = extract_renews_from_index_ops(&gen_storage_changes.transaction_index_changes);
		if !renews.is_empty() {
			log::debug!(
				target: LOG_TARGET,
				"block #{block_number:?} ({parent_hash:?}): case B re-executed, \
				 {} renew hashes",
				renews.len(),
			);
		}

		params.state_action =
			StateAction::ApplyChanges(ConsensusStorageChanges::Changes(gen_storage_changes));

		Ok(RenewHashes::Unverified(renews))
	}

	/// Drops every entry whose data is already in the local TRANSACTION column.
	fn filter_missing(&self, renews: RenewHashes) -> RenewHashes {
		let already_present = |hash: &ContentHash| {
			self.backend.blockchain().has_indexed_transaction((*hash).into()).unwrap_or(false)
		};
		match renews {
			RenewHashes::Verified(set) => RenewHashes::Verified(
				set.into_iter().filter(|(hash, _)| !already_present(hash)).collect(),
			),
			RenewHashes::Unverified(set) =>
				RenewHashes::Unverified(set.into_iter().filter(|hash| !already_present(hash)).collect()),
		}
	}

	/// Resolves every missing entry by delegating to the fetcher's batch API, dispatching to
	/// the verified or unverified path based on the [`RenewHashes`] variant. Returns `Err` if
	/// any entry was not served by any peer.
	async fn fetch_all(
		&self,
		missing: RenewHashes,
	) -> Result<Vec<(ContentHash, Vec<u8>)>, ConsensusError> {
		if missing.is_empty() {
			return Ok(Default::default());
		}

		let (wanted_hashes, acquired) = match missing {
			RenewHashes::Verified(set) => {
				let wants: Vec<(ContentHash, HashingAlgorithm)> = set.into_iter().collect();
				let acquired = self.fetcher.fetch_many(&wants).await.map_err(|e| {
					ConsensusError::Other(format!("bitswap fetch_many: {e}").into())
				})?;
				let hashes: Vec<ContentHash> = wants.into_iter().map(|(h, _)| h).collect();
				(hashes, acquired)
			},
			RenewHashes::Unverified(set) => {
				let wants: Vec<ContentHash> = set.into_iter().collect();
				let acquired =
					self.fetcher.fetch_many_unverified(&wants).await.map_err(|e| {
						ConsensusError::Other(
							format!("bitswap fetch_many_unverified: {e}").into(),
						)
					})?;
				(wants, acquired)
			},
		};

		if acquired.len() != wanted_hashes.len() {
			let missing_count = wanted_hashes.len() - acquired.len();
			return Err(ConsensusError::Other(
				format!(
					"bitswap fetch: {missing_count} of {} entries not served",
					wanted_hashes.len(),
				)
				.into(),
			));
		}

		Ok(wanted_hashes
			.into_iter()
			.map(|hash| {
				let data = acquired
					.get(&hash)
					.expect("all hashes present; len equality verified above; qed")
					.clone();
				(hash, data)
			})
			.collect())
	}

	/// Attaches the fetched `Vec<(ContentHash, Vec<u8>)>` to `params.intermediates` under
	/// [`PREFETCHED_INDEXED_TRANSACTIONS_INTERMEDIATE_KEY`]. The inner client extracts the
	/// payload in `apply_block` and forwards it to the backend, which stores the bytes in the
	/// TRANSACTION column atomically with the block's BODY_INDEX writes.
	///
	/// No-op when `fetched` is empty so we don't pollute the intermediates map.
	fn attach_prefetched(
		params: &mut BlockImportParams<Block>,
		fetched: Vec<(ContentHash, Vec<u8>)>,
	) {
		if fetched.is_empty() {
			return;
		}
		for (hash, _) in &fetched {
			log::info!(
				target: LOG_TARGET,
				"attaching bitswap-fetched indexed transaction {hash:?} to BlockImportParams",
			);
		}
		params.insert_intermediate(PREFETCHED_INDEXED_TRANSACTIONS_INTERMEDIATE_KEY, fetched);
	}

	/// Execute the block via the runtime API to obtain its `StorageChanges`, including the
	/// `transaction_index_changes` host-call output the wrapper needs for tip-discovery.
	///
	/// Mirrors the recipe in `cumulus/client/consensus/aura/.../slot_based/block_import.rs`
	/// minus proof-recording. Caller MUST reassign `params.state_action` to
	/// `ApplyChanges(Changes(_))` of the returned value before forwarding to the inner block
	/// import, otherwise the inner client re-executes (double execution).
	fn execute_block(
		&self,
		params: &BlockImportParams<Block>,
	) -> Result<StorageChanges<HashingFor<Block>>, ConsensusError> {
		let parent_hash = *params.header.parent_hash();
		let body = params.body.clone().unwrap_or_default();
		let block = Block::new(params.header.clone(), body);

		let mut runtime_api = self.client.runtime_api();
		runtime_api.set_call_context(CallContext::Onchain { import: true });

		runtime_api.execute_block(parent_hash, block.into()).map_err(|e| {
			ConsensusError::Other(format!("execute_block: runtime_api.execute_block: {e}").into())
		})?;

		let state = self.client.state_at(parent_hash).map_err(|e| {
			ConsensusError::Other(format!("execute_block: state_at({parent_hash:?}): {e}").into())
		})?;

		let gen_storage_changes = runtime_api.into_storage_changes(&state, parent_hash).map_err(
			|e| ConsensusError::Other(format!("execute_block: into_storage_changes: {e}").into()),
		)?;

		if params.header.state_root() != &gen_storage_changes.transaction_storage_root {
			return Err(ConsensusError::Other(
				format!(
					"execute_block: state root mismatch: header={:?}, executed={:?}",
					params.header.state_root(),
					gen_storage_changes.transaction_storage_root,
				)
				.into(),
			));
		}

		Ok(gen_storage_changes)
	}
}

/// The renew hashes a block needs the wrapper to bitswap-fetch, tagged by the discovery path
/// that produced them.
///
/// `Verified` carries `(ContentHash, HashingAlgorithm)` pairs sourced from the runtime API
/// (`TransactionStorageApi::indexed_transactions`); the algorithm is authoritative and bitswap
/// can verify response integrity by recomputing hashes.
///
/// `Unverified` carries bare `ContentHash`es sourced from `IndexOperation::Renew` host calls
/// (via [`StorageChainBlockImport::execute_block`] or `params.state_action.as_storage_changes()`).
/// Host calls don't carry the hashing algorithm, so bitswap matches responses by request-side
/// CID and skips integrity verification; the caller is responsible for a post-commit
/// runtime-API cross-check to detect malicious peers.
enum RenewHashes {
	Verified(HashSet<(ContentHash, HashingAlgorithm)>),
	Unverified(HashSet<ContentHash>),
}

impl RenewHashes {
	fn is_empty(&self) -> bool {
		match self {
			Self::Verified(s) => s.is_empty(),
			Self::Unverified(s) => s.is_empty(),
		}
	}
}

/// Extract `Renew` content-hashes from a `StorageChanges::transaction_index_changes` host-call
/// log. `Insert` operations are intentionally ignored: their bytes are already in the block body
/// and `apply_index_ops` slices them out directly. Duplicate renew hashes (multi-renew shape)
/// dedupe via the `HashSet`; the inner `apply_index_ops` increments refcount per occurrence
/// using the same fetched bytes.
fn extract_renews_from_index_ops(ops: &[IndexOperation]) -> HashSet<ContentHash> {
	ops.iter()
		.filter_map(|op| match op {
			IndexOperation::Renew { hash, .. } => hash.as_slice().try_into().ok(),
			IndexOperation::Insert { .. } => None,
		})
		.collect()
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
) -> HashSet<(ContentHash, HashingAlgorithm)> {
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
		content_hash: ContentHash,
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
		for algo in
			[HashingAlgorithm::Blake2b256, HashingAlgorithm::Sha2_256, HashingAlgorithm::Keccak256]
		{
			let i = info([0u8; 32], 100, algo, RAW_CID_CODEC, u32::MAX);
			assert!(is_supported(&&i), "{algo:?} should be supported with RAW codec");
		}
	}

	#[test]
	fn is_supported_rejects_non_raw_codec() {
		for algo in
			[HashingAlgorithm::Blake2b256, HashingAlgorithm::Sha2_256, HashingAlgorithm::Keccak256]
		{
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

	#[test]
	fn extract_renews_from_index_ops_returns_only_renew_hashes() {
		let ops = vec![
			IndexOperation::Insert { extrinsic: 0, hash: vec![0xaa; 32], size: 100 },
			IndexOperation::Renew { extrinsic: 1, hash: vec![0xbb; 32] },
			IndexOperation::Insert { extrinsic: 2, hash: vec![0xcc; 32], size: 200 },
			IndexOperation::Renew { extrinsic: 3, hash: vec![0xdd; 32] },
		];
		let renews = extract_renews_from_index_ops(&ops);
		assert_eq!(renews, HashSet::from([[0xbb; 32], [0xdd; 32]]));
	}

	#[test]
	fn extract_renews_from_index_ops_dedupes_duplicate_hashes() {
		let h = [0x42; 32];
		let ops = vec![
			IndexOperation::Renew { extrinsic: 0, hash: h.to_vec() },
			IndexOperation::Renew { extrinsic: 1, hash: h.to_vec() },
			IndexOperation::Renew { extrinsic: 2, hash: h.to_vec() },
		];
		let renews = extract_renews_from_index_ops(&ops);
		assert_eq!(renews, HashSet::from([h]));
	}

	#[test]
	fn extract_renews_from_index_ops_handles_empty_input() {
		let renews = extract_renews_from_index_ops(&[]);
		assert!(renews.is_empty());
	}

	#[test]
	fn extract_renews_from_index_ops_drops_malformed_hash_length() {
		let ops = vec![
			IndexOperation::Renew { extrinsic: 0, hash: vec![0xee; 31] },
			IndexOperation::Renew { extrinsic: 1, hash: vec![0xff; 32] },
			IndexOperation::Renew { extrinsic: 2, hash: vec![0x11; 33] },
		];
		let renews = extract_renews_from_index_ops(&ops);
		assert_eq!(renews, HashSet::from([[0xff; 32]]));
	}
}
