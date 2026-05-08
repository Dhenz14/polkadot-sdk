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
//! Ethereum sync-status types owned by the Ethereum RPC crate.

use derive_more::{From, TryInto};
use pallet_revive::evm::U256;
use serde::{Deserialize, Serialize};

/// Status returned by the Ethereum `eth_syncing` RPC method.
#[derive(Debug, Clone, Serialize, Deserialize, From, TryInto, Eq, PartialEq)]
#[serde(untagged)]
pub enum SyncingStatus {
	/// Detailed sync progress returned while the backing node is syncing.
	SyncingProgress(SyncingProgress),
	/// Boolean sync state returned when the backing node is not syncing.
	Bool(bool),
}

impl Default for SyncingStatus {
	fn default() -> Self {
		SyncingStatus::SyncingProgress(Default::default())
	}
}

/// Progress details returned while the backing node is syncing.
#[derive(Debug, Default, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SyncingProgress {
	/// Current block reached by the backing node.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub current_block: Option<U256>,
	/// Highest known block reported by the backing node.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub highest_block: Option<U256>,
	/// First block in the active sync range.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub starting_block: Option<U256>,
}
