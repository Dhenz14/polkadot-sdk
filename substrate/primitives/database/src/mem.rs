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

//! In-memory implementation of `Database`

use crate::{error, Change, ColumnId, Database, Transaction};
use parking_lot::RwLock;
use std::collections::{hash_map::Entry, HashMap};

#[derive(Default)]
/// This implements `Database` as an in-memory hash map. `commit` is not atomic.
pub struct MemDb(RwLock<HashMap<ColumnId, HashMap<Vec<u8>, (u32, Vec<u8>)>>>);

impl<H> Database<H> for MemDb
where
	H: Clone + AsRef<[u8]>,
{
	fn commit(&self, transaction: Transaction<H>) -> error::Result<()> {
		let mut s = self.0.write();
		for change in transaction.0.into_iter() {
			match change {
				Change::Set(col, key, value) => {
					s.entry(col).or_default().insert(key, (1, value));
				},
				Change::Remove(col, key) => {
					s.entry(col).or_default().remove(&key);
				},
				Change::Store(col, hash, value) => {
					s.entry(col)
						.or_default()
						.entry(hash.as_ref().to_vec())
						.and_modify(|(c, _)| *c += 1)
						.or_insert_with(|| (1, value));
				},
				Change::Reference(col, hash) => {
					if let Entry::Occupied(mut entry) =
						s.entry(col).or_default().entry(hash.as_ref().to_vec())
					{
						entry.get_mut().0 += 1;
					}
				},
				Change::ReferenceCount(col, hash, n) => {
					if let Entry::Occupied(mut entry) =
						s.entry(col).or_default().entry(hash.as_ref().to_vec())
					{
						entry.get_mut().0 += n;
					}
				},
				Change::Release(col, hash) => {
					if let Entry::Occupied(mut entry) =
						s.entry(col).or_default().entry(hash.as_ref().to_vec())
					{
						entry.get_mut().0 -= 1;
						if entry.get().0 == 0 {
							entry.remove();
						}
					}
				},
			}
		}

		Ok(())
	}

	fn get(&self, col: ColumnId, key: &[u8]) -> Option<Vec<u8>> {
		let s = self.0.read();
		s.get(&col).and_then(|c| c.get(key).map(|(_, v)| v.clone()))
	}
}

impl MemDb {
	/// Create a new instance
	pub fn new() -> Self {
		MemDb::default()
	}

	/// Count number of values in a column
	pub fn count(&self, col: ColumnId) -> usize {
		let s = self.0.read();
		s.get(&col).map(|c| c.len()).unwrap_or(0)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const COL: ColumnId = 0;
	const KEY: [u8; 32] = [0x42; 32];
	const DATA: &[u8] = b"payload";

	fn refcount(db: &MemDb, col: ColumnId, key: &[u8]) -> u32 {
		let s = db.0.read();
		s.get(&col).and_then(|c| c.get(key).map(|(rc, _)| *rc)).unwrap_or(0)
	}

	#[test]
	fn reference_count_bumps_existing_entry_by_n() {
		let db = MemDb::new();
		let mut tx = Transaction::<[u8; 32]>::new();
		tx.store(COL, KEY, DATA.to_vec());
		<MemDb as Database<[u8; 32]>>::commit(&db, tx).unwrap();
		assert_eq!(refcount(&db, COL, &KEY), 1);

		let mut tx = Transaction::<[u8; 32]>::new();
		tx.reference_count(COL, KEY, 5);
		<MemDb as Database<[u8; 32]>>::commit(&db, tx).unwrap();
		assert_eq!(refcount(&db, COL, &KEY), 6);
	}

	#[test]
	fn reference_count_in_same_transaction_as_store() {
		let db = MemDb::new();
		let mut tx = Transaction::<[u8; 32]>::new();
		tx.store(COL, KEY, DATA.to_vec());
		tx.reference_count(COL, KEY, 4);
		<MemDb as Database<[u8; 32]>>::commit(&db, tx).unwrap();
		assert_eq!(refcount(&db, COL, &KEY), 5);
	}

	#[test]
	fn reference_count_on_missing_key_is_silent_noop() {
		let db = MemDb::new();
		let mut tx = Transaction::<[u8; 32]>::new();
		tx.reference_count(COL, KEY, 5);
		<MemDb as Database<[u8; 32]>>::commit(&db, tx).unwrap();
		assert_eq!(refcount(&db, COL, &KEY), 0);
		assert_eq!(<MemDb as Database<[u8; 32]>>::get(&db, COL, &KEY), None);
	}

	#[test]
	fn reference_count_zero_is_noop() {
		let db = MemDb::new();
		let mut tx = Transaction::<[u8; 32]>::new();
		tx.store(COL, KEY, DATA.to_vec());
		tx.reference_count(COL, KEY, 0);
		<MemDb as Database<[u8; 32]>>::commit(&db, tx).unwrap();
		assert_eq!(refcount(&db, COL, &KEY), 1);
	}

	#[test]
	fn release_then_reference_count_releases_then_bumps() {
		let db = MemDb::new();
		let mut tx = Transaction::<[u8; 32]>::new();
		tx.store(COL, KEY, DATA.to_vec());
		tx.reference_count(COL, KEY, 9);
		<MemDb as Database<[u8; 32]>>::commit(&db, tx).unwrap();
		assert_eq!(refcount(&db, COL, &KEY), 10);

		for _ in 0..10 {
			let mut tx = Transaction::<[u8; 32]>::new();
			tx.release(COL, KEY);
			<MemDb as Database<[u8; 32]>>::commit(&db, tx).unwrap();
		}
		assert_eq!(<MemDb as Database<[u8; 32]>>::get(&db, COL, &KEY), None);
	}
}
