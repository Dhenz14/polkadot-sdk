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

/// A wrapper around `kvdb::Database` that implements `sp_database::Database` trait
use ::kvdb::{DBTransaction, KeyValueDB};

use crate::{error, Change, ColumnId, Database, Transaction};

struct DbAdapter<D: KeyValueDB + 'static>(D);

fn handle_err<T>(result: std::io::Result<T>) -> T {
	match result {
		Ok(r) => r,
		Err(e) => {
			panic!("Critical database error: {:?}", e);
		},
	}
}

/// Read the reference counter for a key.
fn read_counter(
	db: &dyn KeyValueDB,
	col: ColumnId,
	key: &[u8],
) -> error::Result<(Vec<u8>, Option<u32>)> {
	let mut counter_key = key.to_vec();
	counter_key.push(0);
	Ok(match db.get(col, &counter_key).map_err(|e| error::DatabaseError(Box::new(e)))? {
		Some(data) => {
			let mut counter_data = [0; 4];
			if data.len() != 4 {
				return Err(error::DatabaseError(Box::new(std::io::Error::new(
					std::io::ErrorKind::Other,
					format!("Unexpected counter len {}", data.len()),
				))));
			}
			counter_data.copy_from_slice(&data);
			let counter = u32::from_le_bytes(counter_data);
			(counter_key, Some(counter))
		},
		None => (counter_key, None),
	})
}

/// Commit a transaction to a KeyValueDB.
fn commit_impl<H: Clone + AsRef<[u8]>>(
	db: &dyn KeyValueDB,
	transaction: Transaction<H>,
) -> error::Result<()> {
	let mut tx = DBTransaction::new();
	for change in transaction.0.into_iter() {
		match change {
			Change::Set(col, key, value) => tx.put_vec(col, &key, value),
			Change::Remove(col, key) => tx.delete(col, &key),
			Change::Store(col, key, value) => match read_counter(db, col, key.as_ref())? {
				(counter_key, Some(mut counter)) => {
					counter += 1;
					tx.put(col, &counter_key, &counter.to_le_bytes());
				},
				(counter_key, None) => {
					let d = 1u32.to_le_bytes();
					tx.put(col, &counter_key, &d);
					tx.put_vec(col, key.as_ref(), value);
				},
			},
			Change::Reference(col, key) => {
				if let (counter_key, Some(mut counter)) = read_counter(db, col, key.as_ref())? {
					counter += 1;
					tx.put(col, &counter_key, &counter.to_le_bytes());
				}
			},
			Change::ReferenceCount(col, key, n) => {
				if let (counter_key, Some(mut counter)) = read_counter(db, col, key.as_ref())? {
					counter += n;
					tx.put(col, &counter_key, &counter.to_le_bytes());
				}
			},
			Change::Release(col, key) => {
				if let (counter_key, Some(mut counter)) = read_counter(db, col, key.as_ref())? {
					counter -= 1;
					if counter == 0 {
						tx.delete(col, &counter_key);
						tx.delete(col, key.as_ref());
					} else {
						tx.put(col, &counter_key, &counter.to_le_bytes());
					}
				}
			},
		}
	}
	db.write(tx).map_err(|e| error::DatabaseError(Box::new(e)))
}

/// Wrap generic kvdb-based database into a trait object that implements [`Database`].
pub fn as_database<D, H>(db: D) -> std::sync::Arc<dyn Database<H>>
where
	D: KeyValueDB + 'static,
	H: Clone + AsRef<[u8]>,
{
	std::sync::Arc::new(DbAdapter(db))
}

impl<D: KeyValueDB, H: Clone + AsRef<[u8]>> Database<H> for DbAdapter<D> {
	fn commit(&self, transaction: Transaction<H>) -> error::Result<()> {
		commit_impl(&self.0, transaction)
	}

	fn get(&self, col: ColumnId, key: &[u8]) -> Option<Vec<u8>> {
		handle_err(self.0.get(col, key))
	}

	fn contains(&self, col: ColumnId, key: &[u8]) -> bool {
		handle_err(self.0.has_key(col, key))
	}
}

/// RocksDB-specific adapter that implements `optimize_db` via `force_compact`.
#[cfg(feature = "rocksdb")]
pub struct RocksDbAdapter(kvdb_rocksdb::Database);

#[cfg(feature = "rocksdb")]
impl<H: Clone + AsRef<[u8]>> Database<H> for RocksDbAdapter {
	fn commit(&self, transaction: Transaction<H>) -> error::Result<()> {
		commit_impl(&self.0, transaction)
	}

	fn get(&self, col: ColumnId, key: &[u8]) -> Option<Vec<u8>> {
		handle_err(self.0.get(col, key))
	}

	fn contains(&self, col: ColumnId, key: &[u8]) -> bool {
		handle_err(self.0.has_key(col, key))
	}

	fn optimize_db_col(&self, col: ColumnId) -> error::Result<()> {
		self.0.force_compact(col).map_err(|e| error::DatabaseError(Box::new(e)))
	}
}

/// Wrap RocksDB database into a trait object with `optimize_db` support.
#[cfg(feature = "rocksdb")]
pub fn as_rocksdb_database<H>(db: kvdb_rocksdb::Database) -> std::sync::Arc<dyn Database<H>>
where
	H: Clone + AsRef<[u8]>,
{
	std::sync::Arc::new(RocksDbAdapter(db))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::Transaction;

	const COL: ColumnId = 0;
	const KEY: [u8; 32] = [0x42; 32];
	const DATA: &[u8] = b"payload";

	fn open() -> std::sync::Arc<dyn Database<[u8; 32]>> {
		let kv = kvdb_memorydb::create(1);
		as_database(kv)
	}

	fn refcount(db: &dyn Database<[u8; 32]>) -> u32 {
		let mut count = 0u32;
		while db.contains(COL, &KEY) {
			let mut tx = Transaction::<[u8; 32]>::new();
			tx.release(COL, KEY);
			db.commit(tx).unwrap();
			count += 1;
			if count > 4096 {
				panic!("runaway");
			}
		}
		count
	}

	#[test]
	fn kvdb_reference_count_bumps_existing_entry_by_n() {
		let db = open();
		let mut tx = Transaction::<[u8; 32]>::new();
		tx.store(COL, KEY, DATA.to_vec());
		db.commit(tx).unwrap();

		let mut tx = Transaction::<[u8; 32]>::new();
		tx.reference_count(COL, KEY, 5);
		db.commit(tx).unwrap();

		assert_eq!(refcount(&*db), 6);
	}

	#[test]
	fn kvdb_reference_count_on_missing_key_is_silent_noop() {
		let db = open();
		let mut tx = Transaction::<[u8; 32]>::new();
		tx.reference_count(COL, KEY, 5);
		db.commit(tx).unwrap();
		assert!(!db.contains(COL, &KEY));
	}

	#[test]
	fn kvdb_store_plus_multi_reference_in_one_tx_undercounts() {
		let db = open();
		let mut tx = Transaction::<[u8; 32]>::new();
		tx.store(COL, KEY, DATA.to_vec());
		for _ in 0..4 {
			tx.reference(COL, KEY);
		}
		db.commit(tx).unwrap();

		assert_eq!(
			refcount(&*db),
			1,
			"existing buggy pattern: store + N references in one tx undercounts to 1"
		);
	}

	#[test]
	fn kvdb_store_then_reference_count_in_separate_commits_works() {
		let db = open();
		let mut tx = Transaction::<[u8; 32]>::new();
		tx.store(COL, KEY, DATA.to_vec());
		db.commit(tx).unwrap();

		let mut tx = Transaction::<[u8; 32]>::new();
		tx.reference_count(COL, KEY, 4);
		db.commit(tx).unwrap();

		assert_eq!(refcount(&*db), 5);
	}

	#[test]
	fn kvdb_store_plus_reference_count_in_one_tx_undercounts_due_to_kvdb_read_semantics() {
		let db = open();
		let mut tx = Transaction::<[u8; 32]>::new();
		tx.store(COL, KEY, DATA.to_vec());
		tx.reference_count(COL, KEY, 4);
		db.commit(tx).unwrap();

		assert_eq!(
			refcount(&*db),
			1,
			"single-tx Store+ReferenceCount: ReferenceCount reads pre-commit DB (entry missing) and silently no-ops; \
			 callers must split into two commits when using a freshly Stored key (see helper refactor)"
		);
	}
}
