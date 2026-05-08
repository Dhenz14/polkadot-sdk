// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

pub const BEST_BLOCK_METRIC: &str = "block_height{status=\"best\"}";
pub const FINALIZED_BLOCK_METRIC: &str = "block_height{status=\"finalized\"}";
pub const NODE_ROLE_METRIC: &str = "node_roles";
pub const IS_MAJOR_SYNCING_METRIC: &str = "substrate_sub_libp2p_is_major_syncing";

pub const FULLNODE_ROLE_VALUE: f64 = 1.0;
pub const IDLE_VALUE: f64 = 0.0;

pub const NETWORK_READY_TIMEOUT_SECS: u64 = 180;
pub const METRIC_TIMEOUT_SECS: u64 = 60;
pub const BLOCK_PRODUCTION_TIMEOUT_SECS: u64 = 300;
pub const SYNC_TIMEOUT_SECS: u64 = 180;
pub const LOG_TIMEOUT_SECS: u64 = 60;
pub const LOG_ERROR_TIMEOUT_SECS: u64 = 10;

pub const TEST_DATA_SIZE: usize = 2048;
pub const NODE_LOG_CONFIG: &str = "-lsync=trace,sub-libp2p=trace,litep2p=trace,request-response=trace,transaction-storage=trace,bitswap=trace,storage-chain-block-import=debug,storage-chain-fetcher=debug,db=debug,rpc-spec-v2=debug,state=trace";

pub const RELAY_CHAIN: &str = "westend-local";
pub const PARA_ID: u32 = 2487;

pub const RELAY_BINARY: &str = "polkadot";
pub const PARACHAIN_BINARY: &str = "polkadot-omni-node";
pub const PARACHAIN_CHAIN_SPEC: &str =
	"tests/zombie_ci/storage_chain/fixtures/bulletin-westend-spec.json";
