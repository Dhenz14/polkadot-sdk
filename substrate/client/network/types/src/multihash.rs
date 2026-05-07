// This file is part of Substrate.

// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: GPL-3.0-or-later WITH Classpath-exception-2.0

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

//! [`Multihash`] implemenattion used by substrate. Currently it's a wrapper over
//! multihash used by litep2p, but it can be switched to other implementation if needed.

use litep2p::types::multihash::{Code as LiteP2pCode, Error as LiteP2pError, MultihashDigest as _};
use std::fmt::{self, Debug};

type LiteP2pMultihash = litep2p::types::multihash::Multihash<64>;
const MULTIHASH_IDENTITY_CODE: u64 = 0x00;

/// Default [`Multihash`] implementations. Only hashes used by substrate are defined.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Code {
	/// Identity hasher.
	Identity,
	/// SHA-256 (32-byte hash size).
	Sha2_256,
}

impl Code {
	/// Calculate digest using this [`Code`]'s hashing algorithm.
	pub fn digest(&self, input: &[u8]) -> Multihash {
		match self {
			Code::Identity => Multihash::wrap(MULTIHASH_IDENTITY_CODE, input).expect(
				"identity key enc fits in Multihash<64> since it passed from_public_key_protobuf",
			),
			Code::Sha2_256 => LiteP2pCode::Sha2_256.digest(input).into(),
		}
	}
}

/// Error generated when converting to [`Code`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
	/// Invalid multihash size.
	#[error("invalid multihash size '{0}'")]
	InvalidSize(u64),
	/// The multihash code is not supported.
	#[error("unsupported multihash code '{0:x}'")]
	UnsupportedCode(u64),
	/// Catch-all for other errors emitted when converting `u64` code to enum or parsing multihash
	/// from bytes. Never generated as of multihash-0.17.0.
	#[error("other error: {0}")]
	Other(Box<dyn std::error::Error + Send + Sync>),
}

impl From<LiteP2pError> for Error {
	fn from(error: LiteP2pError) -> Self {
		Self::Other(Box::new(error))
	}
}

impl TryFrom<u64> for Code {
	type Error = Error;

	fn try_from(code: u64) -> Result<Self, Self::Error> {
		match code {
			code if code == LiteP2pCode::Sha2_256.into() => Ok(Code::Sha2_256),
			MULTIHASH_IDENTITY_CODE => Ok(Code::Identity),
			code => Err(Self::Error::UnsupportedCode(code)),
		}
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, Ord, PartialOrd)]
pub struct Multihash {
	multihash: LiteP2pMultihash,
}

impl Multihash {
	/// Multihash code.
	pub fn code(&self) -> u64 {
		self.multihash.code()
	}

	/// Multihash digest.
	pub fn digest(&self) -> &[u8] {
		self.multihash.digest()
	}

	/// Wraps the digest in a multihash.
	pub fn wrap(code: u64, input_digest: &[u8]) -> Result<Self, Error> {
		LiteP2pMultihash::wrap(code, input_digest).map(Into::into).map_err(Into::into)
	}

	/// Parses a multihash from bytes.
	///
	/// You need to make sure the passed in bytes have the length of 64.
	pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
		LiteP2pMultihash::from_bytes(bytes).map(Into::into).map_err(Into::into)
	}

	/// Returns the bytes of a multihash.
	pub fn to_bytes(&self) -> Vec<u8> {
		self.multihash.to_bytes()
	}
}

/// Remove extra layer of nestedness by deferring to the wrapped value's [`Debug`].
impl Debug for Multihash {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		Debug::fmt(&self.multihash, f)
	}
}

impl From<LiteP2pMultihash> for Multihash {
	fn from(multihash: LiteP2pMultihash) -> Self {
		Multihash { multihash }
	}
}

impl From<Multihash> for LiteP2pMultihash {
	fn from(multihash: Multihash) -> Self {
		multihash.multihash
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn code_from_u64() {
		assert_eq!(Code::try_from(0x00).unwrap(), Code::Identity);
		assert_eq!(Code::try_from(0x12).unwrap(), Code::Sha2_256);
		assert!(matches!(Code::try_from(0x01).unwrap_err(), Error::UnsupportedCode(0x01)));
	}

	#[test]
	fn code_into_u64() {
		assert_eq!(u64::from(Code::Identity), 0x00);
		assert_eq!(u64::from(Code::Sha2_256), 0x12);
	}
}
