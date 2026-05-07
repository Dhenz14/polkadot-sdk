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
use log::{debug, trace};
use prost::Message;
use sc_network_types::PeerId;
use sp_transaction_storage_proof::HashingAlgorithm;

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

	/// Fetch data for a content hash from a specific peer via bitswap.
	///
	/// Constructs the CID using the given `hashing` algorithm, sends a WANT-BLOCK message, parses
	/// the response, and verifies the returned blob using the same algorithm. Returns `Ok(None)`
	/// when the peer responds with `DontHave`, `Err(BitswapError::HashMismatch)` if the data
	/// hashes to something other than `content_hash`, and `Err(...)` for protocol/network errors.
	pub async fn fetch<N>(
		&self,
		network: &N,
		peer: PeerId,
		content_hash: [u8; 32],
		hashing: HashingAlgorithm,
	) -> Result<Option<Vec<u8>>, BitswapError>
	where
		N: BitswapRequestSender + ?Sized,
	{
		let cid = Self::cid_for_hash(content_hash, hashing)?;
		let request = BitswapMessage {
			wantlist: Some(Wantlist {
				entries: vec![Entry {
					block: cid.to_bytes(),
					want_type: WantType::Block as i32,
					send_dont_have: true,
					..Default::default()
				}],
				full: false,
			}),
			..Default::default()
		};

		trace!(
			target: LOG_TARGET,
			"client: sending WANT for CID {} (hash 0x{}) to {peer}, protocol {PROTOCOL_NAME}",
			cid,
			hex_encode(&content_hash),
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
					"client: request to {peer} for CID {cid} rejected by network: {err:?}",
				);
				return Err(BitswapError::RequestFailed(err.to_string()));
			},
			Err(err) => {
				debug!(
					target: LOG_TARGET,
					"client: response channel for {peer} (CID {cid}) cancelled: {err}",
				);
				return Err(BitswapError::RequestFailed(err.to_string()));
			},
		};

		let response = BitswapMessage::decode(&payload[..]).map_err(|err| {
			debug!(
				target: LOG_TARGET,
				"client: failed to decode response from {peer} (CID {cid}): {err}",
			);
			BitswapError::DecodeError(err.to_string())
		})?;

		if let Some(block) = response.payload.into_iter().next() {
			let block_cid = Self::cid_from_block_prefix(&block.prefix, &block.data)?;
			if !is_cid_supported(&block_cid) {
				debug!(
					target: LOG_TARGET,
					"client: {peer} returned unsupported CID {block_cid} for WANT {cid}",
				);
				return Err(BitswapError::DecodeError(format!(
					"peer returned unsupported CID {block_cid}",
				)));
			}
			if block_cid != cid {
				debug!(
					target: LOG_TARGET,
					"client: {peer} returned CID {block_cid}, expected {cid}",
				);
				return Err(BitswapError::DecodeError(format!(
					"peer returned unexpected CID {block_cid}",
				)));
			}

			let computed = hashing.hash(&block.data);
			if computed != content_hash {
				debug!(
					target: LOG_TARGET,
					"client: hash mismatch from {peer} (CID {cid}): got 0x{}",
					hex_encode(&computed),
				);
				return Err(BitswapError::HashMismatch);
			}

			debug!(
				target: LOG_TARGET,
				"client: received {} bytes for CID {cid} from {peer}",
				block.data.len(),
			);
			return Ok(Some(block.data));
		}

		for presence in response.block_presences {
			let presence_cid = Cid::read_bytes(presence.cid.as_slice())
				.map_err(|err| BitswapError::DecodeError(err.to_string()))?;
			if presence_cid != cid {
				continue;
			}

			match presence.r#type {
				x if x == BlockPresenceType::DontHave as i32 => {
					debug!(
						target: LOG_TARGET,
						"client: {peer} returned DONT_HAVE for CID {cid}",
					);
					return Ok(None);
				},
				x if x == BlockPresenceType::Have as i32 => {
					debug!(
						target: LOG_TARGET,
						"client: {peer} returned HAVE without data for CID {cid}",
					);
					return Err(BitswapError::DecodeError(
						"peer advertised HAVE without returning block data".into(),
					));
				},
				other => {
					debug!(
						target: LOG_TARGET,
						"client: {peer} returned unknown presence type {other} for CID {cid}",
					);
					return Err(BitswapError::DecodeError(format!(
						"peer returned unknown block presence type {other}",
					)));
				},
			}
		}

		debug!(
			target: LOG_TARGET,
			"client: {peer} returned empty response for CID {cid} (treating as DONT_HAVE)",
		);
		Ok(None)
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

fn hex_encode(bytes: &[u8]) -> String {
	let mut s = String::with_capacity(bytes.len() * 2);
	for b in bytes {
		s.push_str(&format!("{:02x}", b));
	}
	s
}

/// Bitswap client errors.
#[derive(Debug)]
pub enum BitswapError {
	/// Returned data did not match the requested content hash.
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
