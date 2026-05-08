// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Thin RPC helper for the `bitswap_v1_get` JSON-RPC method exposed by every
//! substrate node. Replaces the litep2p-based custom bitswap client; tests
//! should use this module instead of speaking the bitswap wire protocol from
//! outside.
//!
//! # Semantics
//!
//! | State                                          | Return                          |
//! |------------------------------------------------|---------------------------------|
//! | data present in TRANSACTION column             | `Ok(Some(bytes))`               |
//! | data absent, node idle                         | `Ok(None)`                      |
//! | node is major-syncing (retry is meaningful)    | `Err(BitswapRpcError::MajorSyncing)` |
//! | RPC transport error                            | `Err(BitswapRpcError::Transport)` |
//! | hex decode failure (should never happen)       | `Err(BitswapRpcError::Decoding)` |

use anyhow::{anyhow, Result};
use std::time::Duration;
use zombienet_sdk::{
	subxt::{backend::rpc::RpcClient, ext::subxt_rpcs::rpc_params},
	NetworkNode,
};

/// Errors from a `bitswap_v1_get` RPC call.
#[derive(Debug, thiserror::Error)]
pub enum BitswapRpcError {
	#[error("node is major syncing (retry)")]
	MajorSyncing,
	#[error("rpc transport: {0}")]
	Transport(String),
	#[error("hex decoding failed: {0}")]
	Decoding(String),
}

/// Single RPC call to `bitswap_v1_get`.
///
/// Returns `Ok(Some(bytes))` if the node has the data, `Ok(None)` if it
/// explicitly does not (NotFound, node idle), `Err(MajorSyncing)` if the node
/// is catching up.
pub async fn bitswap_v1_get(
	node: &NetworkNode,
	cid: &str,
) -> std::result::Result<Option<Vec<u8>>, BitswapRpcError> {
	let url = node.ws_uri();
	let rpc = RpcClient::from_url(url)
		.await
		.map_err(|e| BitswapRpcError::Transport(format!("connect: {e}")))?;

	match rpc.request::<String>("bitswap_v1_get", rpc_params![cid]).await {
		Ok(hex_str) => {
			let stripped = hex_str.trim_start_matches("0x");
			let bytes =
				hex::decode(stripped).map_err(|e| BitswapRpcError::Decoding(e.to_string()))?;
			Ok(Some(bytes))
		},
		Err(e) => {
			let s = e.to_string();
			if s.contains("-32812") {
				Err(BitswapRpcError::MajorSyncing)
			} else if s.contains("-32810") {
				Ok(None)
			} else {
				Err(BitswapRpcError::Transport(s))
			}
		},
	}
}

/// Assert the node does NOT have the data.
///
/// Waits up to `timeout` for the node to leave major-syncing state, then
/// asserts a single `bitswap_v1_get` returns `Ok(None)`.
pub async fn expect_dont_have(node: &NetworkNode, cid: &str, timeout: Duration) -> Result<()> {
	let deadline = std::time::Instant::now() + timeout;
	while std::time::Instant::now() < deadline {
		match bitswap_v1_get(node, cid).await {
			Ok(None) => return Ok(()),
			Ok(Some(bytes)) => {
				return Err(anyhow!(
					"expect_dont_have({cid}): node has {} bytes but should not",
					bytes.len()
				));
			},
			Err(BitswapRpcError::MajorSyncing) => {
				tokio::time::sleep(Duration::from_secs(1)).await;
				continue;
			},
			Err(other) => return Err(anyhow!("bitswap_v1_get: {other}")),
		}
	}
	Err(anyhow!("expect_dont_have({cid}) timed out after {:?} (node still MajorSyncing)", timeout))
}
