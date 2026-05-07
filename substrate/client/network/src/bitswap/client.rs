// Copyright (C) Parity Technologies (UK) Ltd.
// This file is part of Substrate.
// SPDX-License-Identifier: GPL-3.0-or-later WITH Classpath-exception-2.0

// Substrate is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Substrate is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Substrate. If not, see <https://www.gnu.org/licenses/>.

use crate::{IfDisconnected, NetworkRequest, ProtocolName, RequestFailure};

use cid::{multihash::Multihash as CidMultihash, Cid, Version as CidVersion};
use futures::channel::oneshot;
use log::{debug, trace, warn};
use prost::Message;
use sc_network_types::PeerId;
use sp_transaction_storage_proof::HashingAlgorithm;
use std::collections::HashMap;

const LOG_TARGET: &str = "bitswap";

use super::{
	is_cid_supported,
	schema::bitswap::{
		message::{wantlist::Entry, wantlist::WantType, BlockPresenceType, Wantlist},
		Message as BitswapMessage,
	},
	Prefix, PROTOCOL_NAME,
};

const RAW_CODEC: u64 = 0x55;

/// Maximum number of entries per outbound `WANT-BLOCK` wantlist. Mirrors the inbound
/// substrate handler limit (`MAX_WANTED_BLOCKS` in `bitswap/mod.rs`); larger requests are
/// rejected by the peer.
pub const MAX_WANTED_BLOCKS_PER_REQUEST: usize = 16;

/// Per-CID outcome from a [`BitswapClient::fetch_many`] call.
#[derive(Debug)]
pub enum FetchOutcome {
	/// Peer returned valid bytes whose CID matched the request.
	Block(Vec<u8>),
	/// Peer explicitly indicated it does not have this CID.
	DontHave,
	/// Peer's response contained no acknowledgment for this CID, or the acknowledgment
	/// was malformed (unsolicited block, unsupported CID, unknown presence type).
	Missing,
}

type Multihash = CidMultihash<64>;

/// Outbound request abstraction used by [`BitswapClient`].
///
/// `sc-network-sync` can implement this trait for its `NetworkServiceHandle` wrapper by
/// forwarding to `start_request`, while `sc-network` users can rely on the blanket
/// implementation for [`NetworkRequest`].
pub trait BitswapRequestSender {
	/// Start a request-response exchange with a peer.
	fn start_bitswap_request(
		&self,
		peer: PeerId,
		protocol: ProtocolName,
		payload: Vec<u8>,
		tx: oneshot::Sender<Result<(Vec<u8>, ProtocolName), RequestFailure>>,
		connect: IfDisconnected,
	);
}

impl<T> BitswapRequestSender for T
where
	T: NetworkRequest + ?Sized,
{
	fn start_bitswap_request(
		&self,
		peer: PeerId,
		protocol: ProtocolName,
		payload: Vec<u8>,
		tx: oneshot::Sender<Result<(Vec<u8>, ProtocolName), RequestFailure>>,
		connect: IfDisconnected,
	) {
		self.start_request(peer, protocol, payload, None, tx, connect);
	}
}

/// Bitswap client.
#[derive(Debug, Default)]
pub struct BitswapClient;

impl BitswapClient {
	/// Create a new [`BitswapClient`].
	pub fn new() -> Self {
		Self
	}

	/// Fetch data for one or more content hashes from a single peer in a single
	/// `WANT-BLOCK` request.
	///
	/// Sends a multi-entry wantlist (`WANT-BLOCK`, `send_dont_have = true`) and returns a
	/// per-CID outcome map. Per-CID failures (unsolicited blocks, malformed presences, peer
	/// silence) are isolated to that CID — they never poison other entries in the batch.
	///
	/// Errors out with [`BitswapError::DecodeError`] if `wants` is empty or longer than
	/// [`MAX_WANTED_BLOCKS_PER_REQUEST`]; chunking is the caller's responsibility.
	pub async fn fetch_many<N>(
		&self,
		network: &N,
		peer: PeerId,
		wants: &[([u8; 32], HashingAlgorithm)],
	) -> Result<HashMap<[u8; 32], FetchOutcome>, BitswapError>
	where
		N: BitswapRequestSender + ?Sized,
	{
		if wants.is_empty() {
			return Err(BitswapError::DecodeError("empty wantlist".into()));
		}
		if wants.len() > MAX_WANTED_BLOCKS_PER_REQUEST {
			return Err(BitswapError::DecodeError(format!(
				"wantlist too large: {} > {MAX_WANTED_BLOCKS_PER_REQUEST}",
				wants.len(),
			)));
		}

		let mut wanted: HashMap<Cid, ([u8; 32], HashingAlgorithm)> =
			HashMap::with_capacity(wants.len());
		for &(content_hash, hashing) in wants {
			let cid = Self::cid_for_hash(content_hash, hashing)?;
			wanted.insert(cid, (content_hash, hashing));
		}

		let entries: Vec<Entry> = wanted
			.keys()
			.map(|cid| Entry {
				block: cid.to_bytes(),
				want_type: WantType::Block as i32,
				send_dont_have: true,
				..Default::default()
			})
			.collect();
		let request = BitswapMessage {
			wantlist: Some(Wantlist { entries, full: false }),
			..Default::default()
		};

		trace!(
			target: LOG_TARGET,
			"client: sending WANT-BLOCK for {} CIDs to {peer}, protocol {PROTOCOL_NAME}",
			wanted.len(),
		);

		let (tx, rx) = oneshot::channel();
		network.start_bitswap_request(
			peer,
			ProtocolName::from(PROTOCOL_NAME),
			request.encode_to_vec(),
			tx,
			IfDisconnected::TryConnect,
		);

		let payload = match rx.await {
			Ok(Ok((payload, _))) => payload,
			Ok(Err(err)) => {
				debug!(
					target: LOG_TARGET,
					"client: batch request to {peer} rejected by network: {err:?}",
				);
				return Err(BitswapError::RequestFailed(err.to_string()));
			},
			Err(err) => {
				debug!(
					target: LOG_TARGET,
					"client: batch response channel for {peer} cancelled: {err}",
				);
				return Err(BitswapError::RequestFailed(err.to_string()));
			},
		};

		let response = BitswapMessage::decode(&payload[..]).map_err(|err| {
			debug!(
				target: LOG_TARGET,
				"client: failed to decode batch response from {peer}: {err}",
			);
			BitswapError::DecodeError(err.to_string())
		})?;

		let mut result: HashMap<[u8; 32], FetchOutcome> = HashMap::with_capacity(wanted.len());

		for block in response.payload {
			let block_cid = match Self::cid_from_block_prefix(&block.prefix, &block.data) {
				Ok(cid) => cid,
				Err(err) => {
					debug!(
						target: LOG_TARGET,
						"client: malformed block prefix from {peer}: {err:?}",
					);
					continue;
				},
			};
			if !is_cid_supported(&block_cid) {
				debug!(
					target: LOG_TARGET,
					"client: {peer} returned unsupported CID {block_cid}",
				);
				continue;
			}
			let Some(&(content_hash, _)) = wanted.get(&block_cid) else {
				debug!(
					target: LOG_TARGET,
					"client: {peer} returned unsolicited block for CID {block_cid}",
				);
				continue;
			};
			debug!(
				target: LOG_TARGET,
				"client: {peer} returned {} bytes for CID {block_cid}",
				block.data.len(),
			);
			result.insert(content_hash, FetchOutcome::Block(block.data));
		}

		for presence in response.block_presences {
			let presence_cid = match Cid::read_bytes(presence.cid.as_slice()) {
				Ok(cid) => cid,
				Err(err) => {
					debug!(
						target: LOG_TARGET,
						"client: malformed presence CID from {peer}: {err}",
					);
					continue;
				},
			};
			let Some(&(content_hash, _)) = wanted.get(&presence_cid) else {
				debug!(
					target: LOG_TARGET,
					"client: {peer} returned presence for unrequested CID {presence_cid}",
				);
				continue;
			};
			if result.contains_key(&content_hash) {
				continue;
			}
			match presence.r#type {
				x if x == BlockPresenceType::DontHave as i32 => {
					debug!(
						target: LOG_TARGET,
						"client: {peer} DONT_HAVE for CID {presence_cid}",
					);
					result.insert(content_hash, FetchOutcome::DontHave);
				},
				x if x == BlockPresenceType::Have as i32 => {
					warn!(
						target: LOG_TARGET,
						"client: {peer} advertised HAVE without data for CID {presence_cid}",
					);
					result.insert(content_hash, FetchOutcome::Missing);
				},
				other => {
					warn!(
						target: LOG_TARGET,
						"client: {peer} returned unknown presence type {other} for CID {presence_cid}",
					);
					result.insert(content_hash, FetchOutcome::Missing);
				},
			}
		}

		for (_, &(content_hash, _)) in &wanted {
			result.entry(content_hash).or_insert(FetchOutcome::Missing);
		}

		Ok(result)
	}

	fn cid_for_hash(
		content_hash: [u8; 32],
		hashing: HashingAlgorithm,
	) -> Result<Cid, BitswapError> {
		let multihash = Multihash::wrap(hashing.multihash_code(), &content_hash)
			.map_err(|err| BitswapError::DecodeError(err.to_string()))?;
		Ok(Cid::new_v1(RAW_CODEC, multihash))
	}

	fn cid_from_block_prefix(prefix: &[u8], data: &[u8]) -> Result<Cid, BitswapError> {
		let prefix = decode_prefix(prefix)?;
		let hashing = HashingAlgorithm::from_multihash_code(prefix.mh_type).ok_or_else(|| {
			BitswapError::UnsupportedHashing { multihash_code: prefix.mh_type }
		})?;
		let hash = hashing.hash(data);
		let multihash = Multihash::wrap(prefix.mh_type, &hash)
			.map_err(|err| BitswapError::DecodeError(err.to_string()))?;

		match prefix.version {
			CidVersion::V1 => Ok(Cid::new_v1(prefix.codec, multihash)),
			CidVersion::V0 => Err(BitswapError::DecodeError(
				"bitswap block prefix used unsupported CIDv0".into(),
			)),
		}
	}
}

fn decode_prefix(mut bytes: &[u8]) -> Result<Prefix, BitswapError> {
	let mut read_varint = || -> Result<u64, BitswapError> {
		let (v, rest) = unsigned_varint::decode::u64(bytes)
			.map_err(|err| BitswapError::DecodeError(err.to_string()))?;
		bytes = rest;
		Ok(v)
	};

	let version = read_varint()?;
	let codec = read_varint()?;
	let mh_type = read_varint()?;
	let mh_len = read_varint()?;

	if !bytes.is_empty() {
		return Err(BitswapError::DecodeError("bitswap block prefix had trailing bytes".into()));
	}

	let version = CidVersion::try_from(version)
		.map_err(|_| BitswapError::DecodeError(format!("unsupported CID version {version}")))?;
	let mh_len = u8::try_from(mh_len).map_err(|_| {
		BitswapError::DecodeError(format!("multihash length {mh_len} does not fit into u8"))
	})?;

	Ok(Prefix { version, codec, mh_type, mh_len })
}

/// Bitswap client errors.
#[derive(Debug)]
pub enum BitswapError {
	/// Returned data did not match the requested content hash.
	///
	/// Reserved for API stability; not constructed by [`BitswapClient::fetch_many`], which
	/// surfaces such cases as [`FetchOutcome::Missing`] for the affected CID.
	HashMismatch,
	/// Failed to decode or validate a bitswap payload.
	DecodeError(String),
	/// Request/response exchange failed.
	RequestFailed(String),
	/// Block prefix declared a multihash code that does not map to any supported
	/// `HashingAlgorithm`.
	UnsupportedHashing {
		/// The unrecognised IPFS multihash code.
		multihash_code: u64,
	},
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::RequestFailure;
	use sc_network_types::PeerId;
	use std::{collections::VecDeque, sync::Mutex};

	use super::super::schema::bitswap::message::{
		Block as MessageBlock, BlockPresence, BlockPresenceType,
	};

	struct StubSender(Mutex<VecDeque<Result<Vec<u8>, RequestFailure>>>);

	impl StubSender {
		fn new(responses: impl IntoIterator<Item = Result<Vec<u8>, RequestFailure>>) -> Self {
			Self(Mutex::new(responses.into_iter().collect()))
		}
	}

	impl BitswapRequestSender for StubSender {
		fn start_bitswap_request(
			&self,
			_peer: PeerId,
			_protocol: ProtocolName,
			_payload: Vec<u8>,
			tx: oneshot::Sender<Result<(Vec<u8>, ProtocolName), RequestFailure>>,
			_connect: IfDisconnected,
		) {
			let resp = self
				.0
				.lock()
				.unwrap()
				.pop_front()
				.expect("StubSender: no canned response queued");
			let _ = tx.send(resp.map(|bytes| (bytes, ProtocolName::from(PROTOCOL_NAME))));
		}
	}

	fn prefix_for(hashing: HashingAlgorithm) -> Vec<u8> {
		Prefix {
			version: CidVersion::V1,
			codec: RAW_CODEC,
			mh_type: hashing.multihash_code(),
			mh_len: 32,
		}
		.to_bytes()
	}

	fn cid_for(hash: [u8; 32], hashing: HashingAlgorithm) -> Cid {
		let mh = Multihash::wrap(hashing.multihash_code(), &hash).unwrap();
		Cid::new_v1(RAW_CODEC, mh)
	}

	fn encode_response(
		blocks: &[(HashingAlgorithm, Vec<u8>)],
		presences: &[([u8; 32], HashingAlgorithm, i32)],
	) -> Vec<u8> {
		let payload = blocks
			.iter()
			.map(|(hashing, data)| MessageBlock {
				prefix: prefix_for(*hashing),
				data: data.clone(),
			})
			.collect();
		let block_presences = presences
			.iter()
			.map(|(hash, hashing, ptype)| BlockPresence {
				cid: cid_for(*hash, *hashing).to_bytes(),
				r#type: *ptype,
			})
			.collect();
		BitswapMessage { payload, block_presences, ..Default::default() }.encode_to_vec()
	}

	#[tokio::test]
	async fn fetch_many_returns_blocks_for_all_wanted() {
		let data_a = b"hash-a-payload".to_vec();
		let data_b = b"hash-b-payload".to_vec();
		let data_c = b"hash-c-payload".to_vec();
		let hash_a = HashingAlgorithm::Blake2b256.hash(&data_a);
		let hash_b = HashingAlgorithm::Blake2b256.hash(&data_b);
		let hash_c = HashingAlgorithm::Blake2b256.hash(&data_c);

		let response = encode_response(
			&[
				(HashingAlgorithm::Blake2b256, data_a.clone()),
				(HashingAlgorithm::Blake2b256, data_b.clone()),
				(HashingAlgorithm::Blake2b256, data_c.clone()),
			],
			&[],
		);
		let stub = StubSender::new([Ok(response)]);
		let client = BitswapClient::new();

		let result = client
			.fetch_many(
				&stub,
				PeerId::random(),
				&[
					(hash_a, HashingAlgorithm::Blake2b256),
					(hash_b, HashingAlgorithm::Blake2b256),
					(hash_c, HashingAlgorithm::Blake2b256),
				],
			)
			.await
			.expect("fetch_many should succeed");

		assert_eq!(result.len(), 3);
		assert!(matches!(result.get(&hash_a), Some(FetchOutcome::Block(d)) if *d == data_a));
		assert!(matches!(result.get(&hash_b), Some(FetchOutcome::Block(d)) if *d == data_b));
		assert!(matches!(result.get(&hash_c), Some(FetchOutcome::Block(d)) if *d == data_c));
	}

	#[tokio::test]
	async fn fetch_many_partial_dont_have() {
		let data_a = b"a".to_vec();
		let data_b = b"b".to_vec();
		let hash_a = HashingAlgorithm::Blake2b256.hash(&data_a);
		let hash_b = HashingAlgorithm::Blake2b256.hash(&data_b);
		let hash_c = HashingAlgorithm::Blake2b256.hash(b"c-not-served");

		let response = encode_response(
			&[
				(HashingAlgorithm::Blake2b256, data_a.clone()),
				(HashingAlgorithm::Blake2b256, data_b.clone()),
			],
			&[(hash_c, HashingAlgorithm::Blake2b256, BlockPresenceType::DontHave as i32)],
		);
		let stub = StubSender::new([Ok(response)]);
		let client = BitswapClient::new();

		let result = client
			.fetch_many(
				&stub,
				PeerId::random(),
				&[
					(hash_a, HashingAlgorithm::Blake2b256),
					(hash_b, HashingAlgorithm::Blake2b256),
					(hash_c, HashingAlgorithm::Blake2b256),
				],
			)
			.await
			.unwrap();

		assert_eq!(result.len(), 3);
		assert!(matches!(result.get(&hash_a), Some(FetchOutcome::Block(_))));
		assert!(matches!(result.get(&hash_b), Some(FetchOutcome::Block(_))));
		assert!(matches!(result.get(&hash_c), Some(FetchOutcome::DontHave)));
	}

	#[tokio::test]
	async fn fetch_many_corrupted_data_dropped_as_unsolicited() {
		// Wanted hash is for the correct payload.
		let real_data = b"real-payload".to_vec();
		let wanted_hash = HashingAlgorithm::Blake2b256.hash(&real_data);

		// Peer sends a block whose prefix structure is well-formed but whose data does not hash
		// to wanted_hash. `cid_from_block_prefix` will derive a CID for the corrupted data
		// (different from the wanted CID) and the block falls into "unsolicited block, drop"
		// rather than serving the wanted entry.
		let corrupted_data = b"i-am-not-the-real-payload".to_vec();
		let response = encode_response(
			&[(HashingAlgorithm::Blake2b256, corrupted_data)],
			&[],
		);
		let stub = StubSender::new([Ok(response)]);
		let client = BitswapClient::new();

		let result = client
			.fetch_many(
				&stub,
				PeerId::random(),
				&[(wanted_hash, HashingAlgorithm::Blake2b256)],
			)
			.await
			.unwrap();

		assert_eq!(result.len(), 1);
		assert!(matches!(result.get(&wanted_hash), Some(FetchOutcome::Missing)));
	}

	#[tokio::test]
	async fn fetch_many_unsolicited_block_dropped() {
		let wanted_data = b"wanted".to_vec();
		let wanted_hash = HashingAlgorithm::Blake2b256.hash(&wanted_data);
		let extra_data = b"extra-not-asked-for".to_vec();

		let response = encode_response(
			&[
				(HashingAlgorithm::Blake2b256, wanted_data.clone()),
				(HashingAlgorithm::Blake2b256, extra_data),
			],
			&[],
		);
		let stub = StubSender::new([Ok(response)]);
		let client = BitswapClient::new();

		let result = client
			.fetch_many(
				&stub,
				PeerId::random(),
				&[(wanted_hash, HashingAlgorithm::Blake2b256)],
			)
			.await
			.unwrap();

		assert_eq!(result.len(), 1);
		assert!(matches!(result.get(&wanted_hash), Some(FetchOutcome::Block(d)) if *d == wanted_data));
	}

	#[tokio::test]
	async fn fetch_many_silent_omission_becomes_missing() {
		let data_a = b"a".to_vec();
		let hash_a = HashingAlgorithm::Blake2b256.hash(&data_a);
		let hash_b = HashingAlgorithm::Blake2b256.hash(b"b-omitted");
		let hash_c = HashingAlgorithm::Blake2b256.hash(b"c-omitted");

		let response = encode_response(&[(HashingAlgorithm::Blake2b256, data_a)], &[]);
		let stub = StubSender::new([Ok(response)]);
		let client = BitswapClient::new();

		let result = client
			.fetch_many(
				&stub,
				PeerId::random(),
				&[
					(hash_a, HashingAlgorithm::Blake2b256),
					(hash_b, HashingAlgorithm::Blake2b256),
					(hash_c, HashingAlgorithm::Blake2b256),
				],
			)
			.await
			.unwrap();

		assert_eq!(result.len(), 3);
		assert!(matches!(result.get(&hash_a), Some(FetchOutcome::Block(_))));
		assert!(matches!(result.get(&hash_b), Some(FetchOutcome::Missing)));
		assert!(matches!(result.get(&hash_c), Some(FetchOutcome::Missing)));
	}

	#[tokio::test]
	async fn fetch_many_empty_wants_errors() {
		let stub = StubSender::new(std::iter::empty());
		let client = BitswapClient::new();

		let err = client
			.fetch_many(&stub, PeerId::random(), &[])
			.await
			.expect_err("empty wantlist must error");
		assert!(matches!(err, BitswapError::DecodeError(_)));
	}

	#[tokio::test]
	async fn fetch_many_over_cap_errors() {
		let wants: Vec<_> = (0..(MAX_WANTED_BLOCKS_PER_REQUEST + 1) as u8)
			.map(|i| {
				let mut h = [0u8; 32];
				h[0] = i;
				(h, HashingAlgorithm::Blake2b256)
			})
			.collect();
		let stub = StubSender::new(std::iter::empty());
		let client = BitswapClient::new();

		let err = client
			.fetch_many(&stub, PeerId::random(), &wants)
			.await
			.expect_err("over-cap wantlist must error");
		assert!(matches!(err, BitswapError::DecodeError(_)));
	}

	#[tokio::test]
	async fn fetch_many_request_failure_propagates() {
		let stub = StubSender::new([Err(RequestFailure::NotConnected)]);
		let client = BitswapClient::new();

		let hash = HashingAlgorithm::Blake2b256.hash(b"any");
		let err = client
			.fetch_many(
				&stub,
				PeerId::random(),
				&[(hash, HashingAlgorithm::Blake2b256)],
			)
			.await
			.expect_err("network failure must propagate");
		assert!(matches!(err, BitswapError::RequestFailed(_)));
	}

	#[tokio::test]
	async fn fetch_many_dispatches_per_entry_hashing() {
		let data_b2 = b"blake2b-payload".to_vec();
		let data_sha = b"sha2-256-payload".to_vec();
		let data_kec = b"keccak-256-payload".to_vec();
		let hash_b2 = HashingAlgorithm::Blake2b256.hash(&data_b2);
		let hash_sha = HashingAlgorithm::Sha2_256.hash(&data_sha);
		let hash_kec = HashingAlgorithm::Keccak256.hash(&data_kec);

		let response = encode_response(
			&[
				(HashingAlgorithm::Blake2b256, data_b2.clone()),
				(HashingAlgorithm::Sha2_256, data_sha.clone()),
				(HashingAlgorithm::Keccak256, data_kec.clone()),
			],
			&[],
		);
		let stub = StubSender::new([Ok(response)]);
		let client = BitswapClient::new();

		let result = client
			.fetch_many(
				&stub,
				PeerId::random(),
				&[
					(hash_b2, HashingAlgorithm::Blake2b256),
					(hash_sha, HashingAlgorithm::Sha2_256),
					(hash_kec, HashingAlgorithm::Keccak256),
				],
			)
			.await
			.unwrap();

		assert_eq!(result.len(), 3);
		assert!(matches!(result.get(&hash_b2), Some(FetchOutcome::Block(d)) if *d == data_b2));
		assert!(matches!(result.get(&hash_sha), Some(FetchOutcome::Block(d)) if *d == data_sha));
		assert!(matches!(result.get(&hash_kec), Some(FetchOutcome::Block(d)) if *d == data_kec));
	}
}
