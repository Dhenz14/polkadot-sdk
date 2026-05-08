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
use sp_transaction_storage_proof::{ContentHash, HashingAlgorithm};
use std::collections::HashMap;

const LOG_TARGET: &str = "bitswap";

use super::{
	is_cid_supported,
	schema::bitswap::{
		message::{
			wantlist::{Entry, WantType},
			BlockPresenceType, Wantlist,
		},
		Message as BitswapMessage,
	},
	Prefix, PROTOCOL_NAME,
};

const RAW_CODEC: u64 = 0x55;

/// Maximum entries per `WANT-BLOCK` request. Bigger requests get rejected by the peer
/// (see `MAX_WANTED_BLOCKS` in `bitswap/mod.rs`).
pub const MAX_WANTED_BLOCKS_PER_REQUEST: usize = 16;

/// Per-CID outcome from a [`fetch_many`] call.
#[derive(Debug)]
pub enum FetchOutcome {
	/// Peer returned valid bytes whose CID matched the request.
	Block(Vec<u8>),
	/// Peer explicitly indicated it does not have this CID.
	DontHave,
	/// Peer didn't acknowledge this CID, or its response was malformed.
	Missing,
}

type Multihash = CidMultihash<64>;

/// Outbound bitswap request transport. Blanket-implemented for any [`NetworkRequest`].
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

/// Send one `WANT-BLOCK` request for `wants` to `peer` and classify the response.
///
/// Returns a map with an outcome per requested hash. Bad blocks from the peer affect
/// only their own entry, never others.
///
/// Errors if `wants` is empty or larger than [`MAX_WANTED_BLOCKS_PER_REQUEST`].
pub async fn fetch_many<N>(
	network: &N,
	peer: PeerId,
	wants: &[(ContentHash, HashingAlgorithm)],
) -> Result<HashMap<ContentHash, FetchOutcome>, BitswapError>
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

	let wanted = build_wanted_map(wants)?;
	let response = send_request(network, peer, &wanted).await?;
	Ok(classify_response(response, &wanted, peer))
}

fn build_wanted_map(
	wants: &[(ContentHash, HashingAlgorithm)],
) -> Result<HashMap<Cid, (ContentHash, HashingAlgorithm)>, BitswapError> {
	let mut wanted = HashMap::with_capacity(wants.len());
	for &(content_hash, hashing) in wants {
		let cid = cid_for_hash(content_hash, hashing)?;
		wanted.insert(cid, (content_hash, hashing));
	}
	Ok(wanted)
}

async fn send_request<N>(
	network: &N,
	peer: PeerId,
	wanted: &HashMap<Cid, (ContentHash, HashingAlgorithm)>,
) -> Result<BitswapMessage, BitswapError>
where
	N: BitswapRequestSender + ?Sized,
{
	let entries: Vec<Entry> = wanted
		.keys()
		.map(|cid| Entry {
			block: cid.to_bytes(),
			want_type: WantType::Block as i32,
			send_dont_have: true,
			..Default::default()
		})
		.collect();
	let request =
		BitswapMessage { wantlist: Some(Wantlist { entries, full: false }), ..Default::default() };

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

	BitswapMessage::decode(&payload[..]).map_err(|err| {
		debug!(target: LOG_TARGET, "client: failed to decode batch response from {peer}: {err}");
		BitswapError::DecodeError(err.to_string())
	})
}

fn classify_response(
	response: BitswapMessage,
	wanted: &HashMap<Cid, (ContentHash, HashingAlgorithm)>,
	peer: PeerId,
) -> HashMap<ContentHash, FetchOutcome> {
	let mut result: HashMap<ContentHash, FetchOutcome> = HashMap::with_capacity(wanted.len());

	for block in response.payload {
		let Ok(cid) = cid_from_block_prefix(&block.prefix, &block.data).inspect_err(|err| {
			debug!(target: LOG_TARGET, "client: malformed block prefix from {peer}: {err:?}");
		}) else {
			continue;
		};
		let Some(content_hash) = lookup_wanted(wanted, &cid, peer, "block") else {
			continue;
		};
		debug!(
			target: LOG_TARGET,
			"client: {peer} returned {} bytes for CID {cid}",
			block.data.len(),
		);
		result.insert(content_hash, FetchOutcome::Block(block.data));
	}

	for presence in response.block_presences {
		let Ok(cid) = Cid::read_bytes(presence.cid.as_slice()).inspect_err(|err| {
			debug!(target: LOG_TARGET, "client: malformed presence CID from {peer}: {err}");
		}) else {
			continue;
		};
		let Some(content_hash) = lookup_wanted(wanted, &cid, peer, "presence") else {
			continue;
		};
		if result.contains_key(&content_hash) {
			continue;
		}
		let outcome = if presence.r#type == BlockPresenceType::DontHave as i32 {
			debug!(target: LOG_TARGET, "client: {peer} DONT_HAVE for CID {cid}");
			FetchOutcome::DontHave
		} else {
			warn!(
				target: LOG_TARGET,
				"client: {peer} unexpected presence type {} for CID {cid}",
				presence.r#type,
			);
			FetchOutcome::Missing
		};
		result.insert(content_hash, outcome);
	}

	for &(content_hash, _) in wanted.values() {
		result.entry(content_hash).or_insert(FetchOutcome::Missing);
	}

	result
}

fn cid_for_hash(content_hash: ContentHash, hashing: HashingAlgorithm) -> Result<Cid, BitswapError> {
	let multihash = Multihash::wrap(hashing.multihash_code(), &content_hash)
		.map_err(|err| BitswapError::DecodeError(err.to_string()))?;
	Ok(Cid::new_v1(RAW_CODEC, multihash))
}

fn cid_from_block_prefix(prefix: &[u8], data: &[u8]) -> Result<Cid, BitswapError> {
	let prefix = decode_prefix(prefix)?;
	let hashing = HashingAlgorithm::from_multihash_code(prefix.mh_type)
		.ok_or(BitswapError::UnsupportedHashing { multihash_code: prefix.mh_type })?;
	let hash = hashing.hash(data);
	let multihash = Multihash::wrap(prefix.mh_type, &hash)
		.map_err(|err| BitswapError::DecodeError(err.to_string()))?;

	match prefix.version {
		CidVersion::V1 => Ok(Cid::new_v1(prefix.codec, multihash)),
		CidVersion::V0 => {
			Err(BitswapError::DecodeError("bitswap block prefix used unsupported CIDv0".into()))
		},
	}
}

fn lookup_wanted(
	wanted: &HashMap<Cid, (ContentHash, HashingAlgorithm)>,
	cid: &Cid,
	peer: PeerId,
	role: &str,
) -> Option<ContentHash> {
	if !is_cid_supported(cid) {
		debug!(target: LOG_TARGET, "client: {peer} returned unsupported CID {cid} in {role}");
		return None;
	}
	match wanted.get(cid) {
		Some(&(content_hash, _)) => Some(content_hash),
		None => {
			debug!(target: LOG_TARGET, "client: {peer} returned unsolicited {role} for CID {cid}");
			None
		},
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

	fn cid_for(hash: ContentHash, hashing: HashingAlgorithm) -> Cid {
		let mh = Multihash::wrap(hashing.multihash_code(), &hash).unwrap();
		Cid::new_v1(RAW_CODEC, mh)
	}

	fn encode_response(
		blocks: &[(HashingAlgorithm, Vec<u8>)],
		presences: &[(ContentHash, HashingAlgorithm, i32)],
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

		let result = fetch_many(
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

		let result = fetch_many(
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
		let response = encode_response(&[(HashingAlgorithm::Blake2b256, corrupted_data)], &[]);
		let stub = StubSender::new([Ok(response)]);

		let result =
			fetch_many(&stub, PeerId::random(), &[(wanted_hash, HashingAlgorithm::Blake2b256)])
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

		let result =
			fetch_many(&stub, PeerId::random(), &[(wanted_hash, HashingAlgorithm::Blake2b256)])
				.await
				.unwrap();

		assert_eq!(result.len(), 1);
		assert!(
			matches!(result.get(&wanted_hash), Some(FetchOutcome::Block(d)) if *d == wanted_data)
		);
	}

	#[tokio::test]
	async fn fetch_many_silent_omission_becomes_missing() {
		let data_a = b"a".to_vec();
		let hash_a = HashingAlgorithm::Blake2b256.hash(&data_a);
		let hash_b = HashingAlgorithm::Blake2b256.hash(b"b-omitted");
		let hash_c = HashingAlgorithm::Blake2b256.hash(b"c-omitted");

		let response = encode_response(&[(HashingAlgorithm::Blake2b256, data_a)], &[]);
		let stub = StubSender::new([Ok(response)]);

		let result = fetch_many(
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

		let err = fetch_many(&stub, PeerId::random(), &[])
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

		let err = fetch_many(&stub, PeerId::random(), &wants)
			.await
			.expect_err("over-cap wantlist must error");
		assert!(matches!(err, BitswapError::DecodeError(_)));
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

		let result = fetch_many(
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

	#[tokio::test]
	async fn fetch_many_block_beats_presence_for_same_cid() {
		let data = b"both-block-and-presence".to_vec();
		let hash = HashingAlgorithm::Blake2b256.hash(&data);

		let response = encode_response(
			&[(HashingAlgorithm::Blake2b256, data.clone())],
			&[(hash, HashingAlgorithm::Blake2b256, BlockPresenceType::DontHave as i32)],
		);
		let stub = StubSender::new([Ok(response)]);

		let result = fetch_many(&stub, PeerId::random(), &[(hash, HashingAlgorithm::Blake2b256)])
			.await
			.unwrap();

		assert_eq!(result.len(), 1);
		assert!(matches!(result.get(&hash), Some(FetchOutcome::Block(d)) if *d == data));
	}

	#[tokio::test]
	async fn fetch_many_response_decode_failure() {
		let stub = StubSender::new([Ok(vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff])]);

		let hash = HashingAlgorithm::Blake2b256.hash(b"any");
		let err = fetch_many(&stub, PeerId::random(), &[(hash, HashingAlgorithm::Blake2b256)])
			.await
			.expect_err("malformed response bytes must surface as DecodeError");
		assert!(matches!(err, BitswapError::DecodeError(_)));
	}

	#[tokio::test]
	async fn fetch_many_channel_cancelled_propagates() {
		struct DroppingSender;
		impl BitswapRequestSender for DroppingSender {
			fn start_bitswap_request(
				&self,
				_peer: PeerId,
				_protocol: ProtocolName,
				_payload: Vec<u8>,
				tx: oneshot::Sender<Result<(Vec<u8>, ProtocolName), RequestFailure>>,
				_connect: IfDisconnected,
			) {
				drop(tx);
			}
		}

		let hash = HashingAlgorithm::Blake2b256.hash(b"any");
		let err =
			fetch_many(&DroppingSender, PeerId::random(), &[(hash, HashingAlgorithm::Blake2b256)])
				.await
				.expect_err("dropped channel must surface as RequestFailed");
		assert!(matches!(err, BitswapError::RequestFailed(_)));
	}

	#[tokio::test]
	async fn fetch_many_unsupported_multihash_in_block_dropped() {
		let wanted_data = b"wanted".to_vec();
		let wanted_hash = HashingAlgorithm::Blake2b256.hash(&wanted_data);

		// Peer sends a block whose prefix declares multihash code 0x99 (no `HashingAlgorithm`
		// maps to it). `cid_from_block_prefix` rejects with `UnsupportedHashing` and the block
		// is dropped, leaving the wanted entry unfilled and the backfill marks it `Missing`.
		const UNSUPPORTED_MH_CODE: u64 = 0x99;
		let bad_prefix = Prefix {
			version: CidVersion::V1,
			codec: RAW_CODEC,
			mh_type: UNSUPPORTED_MH_CODE,
			mh_len: 32,
		}
		.to_bytes();

		let mut payload_msg = BitswapMessage::default();
		payload_msg.payload =
			vec![MessageBlock { prefix: bad_prefix, data: b"some-bytes".to_vec() }];
		let response = payload_msg.encode_to_vec();

		let stub = StubSender::new([Ok(response)]);

		let result =
			fetch_many(&stub, PeerId::random(), &[(wanted_hash, HashingAlgorithm::Blake2b256)])
				.await
				.unwrap();

		assert_eq!(result.len(), 1);
		assert!(matches!(result.get(&wanted_hash), Some(FetchOutcome::Missing)));
	}

	#[tokio::test]
	async fn fetch_many_at_exactly_max_wanted_blocks_succeeds() {
		let mut wants: Vec<(ContentHash, HashingAlgorithm)> =
			Vec::with_capacity(MAX_WANTED_BLOCKS_PER_REQUEST);
		let mut blocks: Vec<(HashingAlgorithm, Vec<u8>)> =
			Vec::with_capacity(MAX_WANTED_BLOCKS_PER_REQUEST);
		for i in 0..MAX_WANTED_BLOCKS_PER_REQUEST {
			let data = format!("payload-{i}").into_bytes();
			let hash = HashingAlgorithm::Blake2b256.hash(&data);
			wants.push((hash, HashingAlgorithm::Blake2b256));
			blocks.push((HashingAlgorithm::Blake2b256, data));
		}

		let response = encode_response(&blocks, &[]);
		let stub = StubSender::new([Ok(response)]);

		let result = fetch_many(&stub, PeerId::random(), &wants)
			.await
			.expect("exactly MAX_WANTED_BLOCKS_PER_REQUEST must succeed");

		assert_eq!(result.len(), MAX_WANTED_BLOCKS_PER_REQUEST);
		for (hash, _) in &wants {
			assert!(matches!(result.get(hash), Some(FetchOutcome::Block(_))));
		}
	}

	#[tokio::test]
	async fn fetch_many_malformed_presence_cid_dropped() {
		let wanted_data = b"wanted".to_vec();
		let wanted_hash = HashingAlgorithm::Blake2b256.hash(&wanted_data);

		// Peer puts garbage bytes in `presence.cid`. `Cid::read_bytes` errors, the presence is
		// dropped, the wanted entry is backfilled as `Missing`.
		let mut response_msg = BitswapMessage::default();
		response_msg.block_presences = vec![BlockPresence {
			cid: vec![0xde, 0xad, 0xbe, 0xef],
			r#type: BlockPresenceType::DontHave as i32,
		}];
		let response = response_msg.encode_to_vec();
		let stub = StubSender::new([Ok(response)]);

		let result =
			fetch_many(&stub, PeerId::random(), &[(wanted_hash, HashingAlgorithm::Blake2b256)])
				.await
				.unwrap();

		assert_eq!(result.len(), 1);
		assert!(matches!(result.get(&wanted_hash), Some(FetchOutcome::Missing)));
	}

	#[tokio::test]
	async fn fetch_many_mostly_presences_response() {
		let served_data = b"the-only-served".to_vec();
		let served_hash = HashingAlgorithm::Blake2b256.hash(&served_data);
		let pruned_a = HashingAlgorithm::Blake2b256.hash(b"pruned-a");
		let pruned_b = HashingAlgorithm::Blake2b256.hash(b"pruned-b");
		let pruned_c = HashingAlgorithm::Blake2b256.hash(b"pruned-c");
		let pruned_d = HashingAlgorithm::Blake2b256.hash(b"pruned-d");

		let response = encode_response(
			&[(HashingAlgorithm::Blake2b256, served_data.clone())],
			&[
				(pruned_a, HashingAlgorithm::Blake2b256, BlockPresenceType::DontHave as i32),
				(pruned_b, HashingAlgorithm::Blake2b256, BlockPresenceType::DontHave as i32),
				(pruned_c, HashingAlgorithm::Blake2b256, BlockPresenceType::DontHave as i32),
				(pruned_d, HashingAlgorithm::Blake2b256, BlockPresenceType::DontHave as i32),
			],
		);
		let stub = StubSender::new([Ok(response)]);

		let result = fetch_many(
			&stub,
			PeerId::random(),
			&[
				(served_hash, HashingAlgorithm::Blake2b256),
				(pruned_a, HashingAlgorithm::Blake2b256),
				(pruned_b, HashingAlgorithm::Blake2b256),
				(pruned_c, HashingAlgorithm::Blake2b256),
				(pruned_d, HashingAlgorithm::Blake2b256),
			],
		)
		.await
		.unwrap();

		assert_eq!(result.len(), 5);
		assert!(
			matches!(result.get(&served_hash), Some(FetchOutcome::Block(d)) if *d == served_data)
		);
		assert!(matches!(result.get(&pruned_a), Some(FetchOutcome::DontHave)));
		assert!(matches!(result.get(&pruned_b), Some(FetchOutcome::DontHave)));
		assert!(matches!(result.get(&pruned_c), Some(FetchOutcome::DontHave)));
		assert!(matches!(result.get(&pruned_d), Some(FetchOutcome::DontHave)));
	}
}
