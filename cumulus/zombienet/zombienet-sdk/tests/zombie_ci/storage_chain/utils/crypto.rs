// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

use blake2::{digest::consts::U32, Blake2b, Digest};

pub fn blake2_256(data: &[u8]) -> [u8; 32] {
	let mut hasher = Blake2b::<U32>::new();
	hasher.update(data);
	let result = hasher.finalize();
	let mut output = [0u8; 32];
	output.copy_from_slice(&result);
	output
}

#[cfg(feature = "generate-snapshots")]
pub fn twox_128(data: &[u8]) -> [u8; 16] {
	use std::hash::Hasher;
	let mut h0 = twox_hash::XxHash64::with_seed(0);
	let mut h1 = twox_hash::XxHash64::with_seed(1);
	h0.write(data);
	h1.write(data);
	let r0 = h0.finish();
	let r1 = h1.finish();
	let mut result = [0u8; 16];
	result[0..8].copy_from_slice(&r0.to_le_bytes());
	result[8..16].copy_from_slice(&r1.to_le_bytes());
	result
}

#[cfg(feature = "generate-snapshots")]
pub fn retention_period_storage_key() -> Vec<u8> {
	let mut key = Vec::new();
	key.extend_from_slice(&twox_128(b"TransactionStorage"));
	key.extend_from_slice(&twox_128(b"RetentionPeriod"));
	key
}

pub fn hash_to_cid(hash: &[u8; 32]) -> String {
	use cid::Cid;
	use multihash::Multihash;
	const BLAKE2B_256: u64 = 0xb220;
	const RAW_CODEC: u64 = 0x55;
	let mh = Multihash::<64>::wrap(BLAKE2B_256, hash).expect("Valid multihash");
	Cid::new_v1(RAW_CODEC, mh).to_string()
}

pub fn generate_test_data(size: usize, pattern: &[u8]) -> Vec<u8> {
	let mut data = Vec::with_capacity(size);
	while data.len() < size {
		let remaining = size - data.len();
		if remaining >= pattern.len() {
			data.extend_from_slice(pattern);
		} else {
			data.extend_from_slice(&pattern[..remaining]);
		}
	}
	data
}
