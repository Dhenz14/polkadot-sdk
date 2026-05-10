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
//! # The bug this wrapper exists to fix
//!
//! Storage-chain parachains track indexed-data refcounts in `sc-client-db`'s TRANSACTION column.
//! When a tip block contains a `Renew` operation for indexed data the local node has never
//! stored, the inner block import calls `transaction.reference()` on a missing key — a silent
//! no-op in kvdb — and the refcount stays at zero. Subsequent prune cycles then drop entries the
//! runtime considered live.
//!
//! # Discovery dispatch (case A / B / C)
//!
//! The wrapper has to discover **which content hashes the block's `Renew` operations refer to**
//! before it can bitswap-fetch missing payloads. There are three structurally different sources
//! of that information; [`StorageChainBlockImport::classify_renew_hashes`] dispatches between
//! them:
//!
//! - **Case A** (already-executed peek). When `params.state_action` is
//!   `ApplyChanges(Changes(_))`, the upstream proposer (or a previous wrapper invocation) has
//!   already executed the block and populated `StorageChanges`. Read `transaction_index_changes`
//!   directly. Cheapest path; the typical case for `BlockOrigin::Own` parachain blocks because
//!   the cumulus collator pre-executes them.
//!
//! - **Case B** (re-execution). For network-imported tip blocks (default
//!   `state_action == StateAction::Execute`), execute the block ourselves via the runtime API
//!   and read the resulting `transaction_index_changes`. Then reassign `params.state_action` to
//!   `ApplyChanges(Changes(gen_storage_changes))` so the inner block-import skips re-execution.
//!   Mirrors the SlotBased recipe at
//!   `cumulus/client/consensus/aura/src/collators/slot_based/block_import.rs`.
//!
//! - **Case C** (runtime-API path). For gap-sync blocks (already committed against on-chain
//!   state), `TransactionStorageApi::indexed_transactions(block_n)` returns authoritative
//!   metadata. Currently unreachable in PR-1 because [`Self::should_intercept`] rejects
//!   `BlockOrigin::GapSync`; PR-2 widens the gate.
//!
//! Cases A and B both source from `IndexOperation::Renew` host calls — these carry only the
//! 32-byte content hash, not the hashing algorithm. Case C sources from the runtime API which
//! carries `(content_hash, hashing)` per entry.
//!
//! # Bitswap fetch dispatch (verified vs unverified)
//!
//! Cases A/B → [`bitswap::fetch_many_unverified`]: matches responses by request-side CID,
//! skipping the in-bitswap hash-recompute (since we don't know the algorithm). Order-based
//! correlation across multi-WANT batches works because the substrate server preserves request
//! order in its response payload and emits DontHave presences for missing entries.
//!
//! Case C → [`bitswap::fetch_many`]: standard verified path with per-entry `HashingAlgorithm`.
//!
//! # Post-commit verification
//!
//! Unverified-path fetches are vulnerable to a malicious peer returning bytes whose hash
//! doesn't match the runtime-declared `content_hash`. After `inner.import_block(params)`
//! commits the block, [`Self::verify_post_commit`] calls
//! `TransactionStorageApi::indexed_transactions(block_hash, block_number)` against the
//! now-committed block's state and verifies `info.hashing.hash(bytes) == info.content_hash`
//! for each fetched hash. Mismatches are logged (not fatal) in PR-1; PR-3+ adds re-fetch and
//! atomic replace.
//!
//! # Pipeline summary
//!
//! 1. [`Self::should_intercept`] gates the wrapper to tip blocks with bodies on
//!    `TransactionStorageApi >= 2` runtimes.
//! 2. [`Self::classify_renew_hashes`] dispatches across cases A/B/C and yields a
//!    [`RenewHashes`] enum tagged with the discovery path.
//! 3. [`Self::filter_missing`] drops hashes already in the local TRANSACTION column.
//! 4. [`Self::fetch_all`] dispatches to verified or unverified bitswap based on the variant
//!    and returns a [`FetchedRenews`] with both the payload bytes and the subset of fetched
//!    hashes that need post-commit verification.
//! 5. [`Self::attach_prefetched`] attaches the payload to `BlockImportParams::intermediates`
//!    under [`PREFETCHED_INDEXED_TRANSACTIONS_INTERMEDIATE_KEY`]. The inner client extracts
//!    it in `apply_block` and forwards to the backend, which writes the bytes to the
//!    TRANSACTION column in the **same atomic commit** as the block's BODY_INDEX entries.
//!    `apply_index_ops::Renew`'s `transaction.reference()` then balances against the
//!    per-occurrence prune-time `transaction.release()`.
//! 6. After `inner.import_block` succeeds, [`Self::verify_post_commit`] cross-checks any
//!    unverified-path fetches against the runtime API.
//!
//! See `Renew Pipeline Host Call Refresher` in the design notes for the full rationale on why
//! re-execution is required at the tip (the runtime API for tip blocks reads parent state, in
//! which the tip's `Transactions::<T>::insert(N, _)` has not yet run).

mod fetcher;

pub use fetcher::{
	BitswapPeerSource, FetchError, IndexedTransactionFetcher, NetworkHandle, SyncingHandle,
};

use sc_client_api::backend::{
	Backend as BackendT, TrieCacheContext, PREFETCHED_INDEXED_TRANSACTIONS_INTERMEDIATE_KEY,
};
use sc_client_db::{
	classify_indexed_extrinsics, Backend, ClassifiedExtrinsic, IndexedTransactionMeta,
};
use sc_consensus::{
	BlockCheckParams, BlockImport, BlockImportParams, ImportResult, StateAction,
	StorageChanges as ConsensusStorageChanges,
};
use sp_api::{ApiExt, CallApiAt, CallContext, Core, ProofRecorder, ProvideRuntimeApi};
use sp_blockchain::Backend as BlockchainBackendT;
use sp_consensus::{BlockOrigin, Error as ConsensusError};
use sp_runtime::traits::{Block as BlockT, HashingFor, Header as HeaderT};
use sp_state_machine::{IndexOperation, StorageChanges};
use sp_transaction_storage_proof::{
	runtime_api::TransactionStorageApi, ContentHash, HashingAlgorithm, IndexedTransactionInfo,
};
use sp_trie::proof_size_extension::{ProofSizeExt, RecordingProofSizeProvider};
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
		let FetchedRenews { payload, unverified_hashes } = self.fetch_all(missing).await?;
		Self::attach_prefetched(&mut params, payload);

		let block_hash = params.post_hash();
		let block_number = *params.header.number();

		let result = self.inner.import_block(params).await?;

		if matches!(result, ImportResult::Imported(_)) && !unverified_hashes.is_empty() {
			self.verify_post_commit(block_hash, block_number, &unverified_hashes);
		}

		Ok(result)
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
	///
	/// The returned [`FetchedRenews::unverified_hashes`] lists which fetched hashes came from
	/// the unverified bitswap path (case A or case B); the caller passes those to
	/// [`Self::verify_post_commit`] after the inner import succeeds.
	async fn fetch_all(&self, missing: RenewHashes) -> Result<FetchedRenews, ConsensusError> {
		if missing.is_empty() {
			return Ok(FetchedRenews::default());
		}

		let (wanted_hashes, acquired, was_unverified) = match missing {
			RenewHashes::Verified(set) => {
				let wants: Vec<(ContentHash, HashingAlgorithm)> = set.into_iter().collect();
				let acquired = self.fetcher.fetch_many(&wants).await.map_err(|e| {
					ConsensusError::Other(format!("bitswap fetch_many: {e}").into())
				})?;
				let hashes: Vec<ContentHash> = wants.into_iter().map(|(h, _)| h).collect();
				(hashes, acquired, false)
			},
			RenewHashes::Unverified(set) => {
				let wants: Vec<ContentHash> = set.into_iter().collect();
				let acquired =
					self.fetcher.fetch_many_unverified(&wants).await.map_err(|e| {
						ConsensusError::Other(
							format!("bitswap fetch_many_unverified: {e}").into(),
						)
					})?;
				(wants, acquired, true)
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

		let payload: Vec<(ContentHash, Vec<u8>)> = wanted_hashes
			.iter()
			.map(|hash| {
				let data = acquired
					.get(hash)
					.expect("all hashes present; len equality verified above; qed")
					.clone();
				(*hash, data)
			})
			.collect();

		let unverified_hashes = if was_unverified { wanted_hashes } else { Vec::new() };
		Ok(FetchedRenews { payload, unverified_hashes })
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
	/// Mirrors the recipe in `cumulus/client/consensus/aura/.../slot_based/block_import.rs`.
	/// Caller MUST reassign `params.state_action` to `ApplyChanges(Changes(_))` of the returned
	/// value before forwarding to the inner block import, otherwise the inner client re-executes
	/// (double execution).
	///
	/// Registers a fresh `ProofRecorder` and `ProofSizeExt`. Cumulus parachain runtimes consult
	/// `ProofSizeExt::storage_proof_size()` during execution and feed the result into weight /
	/// fee computations that affect committed state. Without an active recorder, the runtime
	/// observes `0` while the proposer recorded actual sizes — the resulting state root differs
	/// and `frame_executive::final_checks` panics with "Storage root must match that calculated."
	fn execute_block(
		&self,
		params: &BlockImportParams<Block>,
	) -> Result<StorageChanges<HashingFor<Block>>, ConsensusError> {
		let parent_hash = *params.header.parent_hash();
		let body = params.body.clone().unwrap_or_default();
		let block = Block::new(params.header.clone(), body);

		let recorder = ProofRecorder::<Block>::default();
		let proof_size_recorder = RecordingProofSizeProvider::new(recorder.clone());

		let mut runtime_api = self.client.runtime_api();
		runtime_api.set_call_context(CallContext::Onchain { import: true });
		runtime_api.record_proof_with_recorder(recorder);
		runtime_api.register_extension(ProofSizeExt::new(proof_size_recorder));

		runtime_api.execute_block(parent_hash, block.into()).map_err(|e| {
			ConsensusError::Other(format!("execute_block: runtime_api.execute_block: {e}").into())
		})?;

		let state = self.backend.state_at(parent_hash, TrieCacheContext::Trusted).map_err(
			|e| {
				ConsensusError::Other(
					format!("execute_block: state_at({parent_hash:?}): {e}").into(),
				)
			},
		)?;

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

	/// Cross-check unverified-path bitswap fetches against the runtime's authoritative metadata,
	/// now that the block is committed and `TransactionStorageApi::indexed_transactions` returns
	/// real `(content_hash, hashing)` pairs for the committed block.
	///
	/// **Always returns `Ok(())`.** Mismatches are logged (warn for unknown-to-runtime, error for
	/// declared-algorithm-doesn't-match-bytes) but never propagated. PR-1 ships this as
	/// detect-and-log only; PR-3+ will add re-fetch-and-replace on mismatch.
	///
	/// Called from [`Self::import_block`] only after `inner.import_block(params).await?` returns
	/// successfully, so `Transactions::<T>::get(block_number)` is populated against the now-
	/// committed `block_hash`'s state (per `pallet-transaction-storage::on_finalize`).
	fn verify_post_commit(
		&self,
		block_hash: Block::Hash,
		block_number: <<Block as BlockT>::Header as HeaderT>::Number,
		unverified_hashes: &[ContentHash],
	) {
		if unverified_hashes.is_empty() {
			return;
		}

		let infos = match self
			.client
			.runtime_api()
			.indexed_transactions(block_hash, block_number)
		{
			Ok(infos) => infos,
			Err(e) => {
				log::warn!(
					target: LOG_TARGET,
					"post-commit verify: runtime API indexed_transactions failed for block #{block_number:?} ({block_hash:?}): {e}",
				);
				return;
			},
		};

		let lookup: std::collections::HashMap<ContentHash, &IndexedTransactionInfo> =
			infos.iter().map(|info| (info.content_hash, info)).collect();

		for &content_hash in unverified_hashes {
			let Some(info) = lookup.get(&content_hash) else {
				log::warn!(
					target: LOG_TARGET,
					"post-commit verify: fetched {content_hash:?} not declared by runtime at block #{block_number:?}",
				);
				continue;
			};

			let bytes = match self.backend.blockchain().indexed_transaction(content_hash.into()) {
				Ok(Some(bytes)) => bytes,
				Ok(None) => {
					log::warn!(
						target: LOG_TARGET,
						"post-commit verify: backend has no bytes for {content_hash:?} at block #{block_number:?}",
					);
					continue;
				},
				Err(e) => {
					log::warn!(
						target: LOG_TARGET,
						"post-commit verify: backend lookup failed for {content_hash:?}: {e}",
					);
					continue;
				},
			};

			let observed = info.hashing.hash(&bytes);
			if observed != info.content_hash {
				log::error!(
					target: LOG_TARGET,
					"post-commit verify FAILED at block #{block_number:?}: \
					 declared_algo={:?}, expected_content_hash={:?}, observed={:?} \
					 (peer returned bytes that don't hash to the runtime-declared content_hash)",
					info.hashing,
					info.content_hash,
					observed,
				);
			}
		}
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

/// Result of [`StorageChainBlockImport::fetch_all`].
///
/// `payload` is the `(ContentHash, Vec<u8>)` pairs to attach to `BlockImportParams::intermediates`.
///
/// `unverified_hashes` is the subset of fetched hashes that came from the unverified bitswap
/// path; the caller is expected to run [`StorageChainBlockImport::verify_post_commit`] over
/// these hashes after the inner block-import commits, to detect malicious peers that returned
/// bytes whose hash doesn't match the runtime-declared algorithm.
#[derive(Default)]
struct FetchedRenews {
	payload: Vec<(ContentHash, Vec<u8>)>,
	unverified_hashes: Vec<ContentHash>,
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
