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

//! Wire-compatible mirror of bulletin-chain's `IndexedTransactionsApi`.
//!
//! The trait name and method signature must match bulletin-chain exactly so
//! that the runtime-API id hash computed by `decl_runtime_apis!` resolves to
//! the same WASM entry point. Field order and SCALE encoding of
//! [`IndexedTransactionInfo`] must likewise match bulletin-chain's definition.

use codec::{Decode, Encode};

pub type ContentHash = [u8; 32];

pub type CidCodec = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Encode, Decode, scale_info::TypeInfo)]
#[non_exhaustive]
pub enum HashingAlgorithm {
	Blake2b256,
	Sha2_256,
	Keccak256,
}

#[derive(Clone, Debug, PartialEq, Eq, Encode, Decode, scale_info::TypeInfo)]
pub struct IndexedTransactionInfo {
	pub content_hash: ContentHash,
	pub size: u32,
	pub hashing: HashingAlgorithm,
	pub cid_codec: CidCodec,
}

sp_api::decl_runtime_apis! {
	pub trait IndexedTransactionsApi {
		fn indexed_transactions(block: u32) -> Option<Vec<IndexedTransactionInfo>>;
	}
}
