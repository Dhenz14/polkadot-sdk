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
use zombienet_sdk::subxt::backend::rpc::RpcClient;
use zombienet_sdk::subxt::ext::subxt_rpcs::rpc_params;
use zombienet_sdk::NetworkNode;

use super::crypto::{blake2_256, hash_to_cid};

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
            let bytes = hex::decode(stripped)
                .map_err(|e| BitswapRpcError::Decoding(e.to_string()))?;
            Ok(Some(bytes))
        }
        Err(e) => {
            let s = e.to_string();
            if s.contains("-32812") {
                Err(BitswapRpcError::MajorSyncing)
            } else if s.contains("-32810") {
                Ok(None)
            } else {
                Err(BitswapRpcError::Transport(s))
            }
        }
    }
}

/// Poll until the node has the data or the deadline elapses.
///
/// Treats both `MajorSyncing` and `Ok(None)` as "not yet" within the polling
/// budget. Verifies the returned bytes match `expected`.
pub async fn expect_have(
    node: &NetworkNode,
    cid: &str,
    expected: &[u8],
    timeout: Duration,
) -> Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_err: Option<String> = None;
    while std::time::Instant::now() < deadline {
        match bitswap_v1_get(node, cid).await {
            Ok(Some(bytes)) => {
                if bytes == expected {
                    return Ok(());
                }
                return Err(anyhow!(
                    "bitswap_v1_get({cid}) returned {} bytes but they do not match expected {} bytes",
                    bytes.len(),
                    expected.len()
                ));
            }
            Ok(None) => {
                last_err = Some(format!("NotFound at {cid}"));
            }
            Err(BitswapRpcError::MajorSyncing) => {
                last_err = Some("MajorSyncing".to_string());
            }
            Err(other) => return Err(anyhow!("bitswap_v1_get: {other}")),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(anyhow!(
        "expect_have({cid}) timed out after {:?}; last status: {}",
        timeout,
        last_err.unwrap_or_else(|| "unknown".into())
    ))
}

/// Assert the node does NOT have the data.
///
/// Waits up to `timeout` for the node to leave major-syncing state, then
/// asserts a single `bitswap_v1_get` returns `Ok(None)`.
pub async fn expect_dont_have(
    node: &NetworkNode,
    cid: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        match bitswap_v1_get(node, cid).await {
            Ok(None) => return Ok(()),
            Ok(Some(bytes)) => {
                return Err(anyhow!(
                    "expect_dont_have({cid}): node has {} bytes but should not",
                    bytes.len()
                ));
            }
            Err(BitswapRpcError::MajorSyncing) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(other) => return Err(anyhow!("bitswap_v1_get: {other}")),
        }
    }
    Err(anyhow!(
        "expect_dont_have({cid}) timed out after {:?} (node still MajorSyncing)",
        timeout
    ))
}

// ---- Compatibility shims matching the old utils/bitswap.rs API surface ----

/// Verify a node has the data by content (hash → CID → RPC check).
/// Compatible with the old `verify_node_bitswap` signature.
pub async fn verify_node_bitswap(
    node: &NetworkNode,
    expected_data: &[u8],
    timeout_secs: u64,
    node_name: &str,
) -> Result<()> {
    let hash = blake2_256(expected_data);
    let cid = hash_to_cid(&hash);
    log::info!(
        "verify_node_bitswap on {}: CID={} expected_len={}",
        node_name, cid, expected_data.len()
    );
    expect_have(node, &cid, expected_data, Duration::from_secs(timeout_secs))
        .await
        .map_err(|e| anyhow!("verify_node_bitswap({node_name}): {e}"))
}

/// Assert a node does NOT have the data. Compatible with the old
/// `expect_bitswap_dont_have` signature.
pub async fn expect_bitswap_dont_have(
    node: &NetworkNode,
    expected_data: &[u8],
    timeout_secs: u64,
    node_name: &str,
) -> Result<()> {
    let hash = blake2_256(expected_data);
    let cid = hash_to_cid(&hash);
    log::info!("expect_bitswap_dont_have on {}: CID={}", node_name, cid);
    expect_dont_have(node, &cid, Duration::from_secs(timeout_secs))
        .await
        .map_err(|e| anyhow!("expect_bitswap_dont_have({node_name}): {e}"))
}
