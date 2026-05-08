// This file is part of Substrate.

// Copyright (C) Parity Technologies (UK) Ltd.
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
//! Ethereum log types owned by the Ethereum RPC crate.

use pallet_revive::evm::{Address, Bytes, H256, U256};
use serde::{Deserialize, Serialize};

/// Log entry emitted by an Ethereum transaction.
#[derive(Debug, Default, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Log {
	/// Address of the contract that emitted the log.
	pub address: Address,
	/// Hash of the block containing the transaction that emitted the log.
	pub block_hash: H256,
	/// Number of the block containing the transaction that emitted the log.
	pub block_number: U256,
	/// Non-indexed event payload emitted with the log.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub data: Option<Bytes>,
	/// Index of the log within the block.
	pub log_index: U256,
	/// Indicates whether this log was removed by a chain reorganization.
	#[serde(default)]
	pub removed: bool,
	/// Indexed event topics attached to the log.
	#[serde(default)]
	pub topics: Vec<H256>,
	/// Hash of the transaction that emitted the log.
	pub transaction_hash: H256,
	/// Index of the transaction that emitted the log within its block.
	pub transaction_index: U256,
}

/// Result returned by polling Ethereum filters.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(untagged)]
pub enum FilterResults {
	/// Newly observed block or transaction hashes.
	Hashes(Vec<H256>),
	/// Newly observed logs.
	Logs(Vec<Log>),
}

impl Default for FilterResults {
	fn default() -> Self {
		FilterResults::Hashes(Default::default())
	}
}
