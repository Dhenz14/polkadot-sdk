// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

pub mod bitswap_rpc;
pub mod config;
pub mod crypto;
pub mod fixture;
pub mod network;
pub mod sync;
pub mod tx;

pub use bitswap_rpc::*;
pub use config::*;
pub use crypto::*;
pub use fixture::*;
pub use network::*;
pub use sync::*;
pub use tx::*;

pub use crate::utils::initialize_network;

#[macro_export]
macro_rules! test_log {
	($test_name:expr, $($arg:tt)*) => {
		log::info!(target: "tests::parachain_sync_storage", "[{}] {}", $test_name, format_args!($($arg)*))
	};
}
