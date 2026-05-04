//! Refcount investigation for parity-db ref-counted columns.
//!
//! Standalone integration tests that probe the actual reference count produced by various
//! commit patterns on a `ref_counted + preimage + uniform` ParityDB column matching
//! substrate's TRANSACTION column config.
//!
//! Each test prints the EXPECTED vs ACTUAL refcount and the bump pattern used.
//!
//! Run with:
//!     cargo test -p sc-client-db --test refcount_anomaly -- --nocapture
//!
//! Refcount is measured by:
//! 1) closing the DB to flush all background work,
//! 2) reopening it,
//! 3) issuing `Dereference` ops one at a time, closing+reopening between each to defeat
//!    parity-db's commit-overlay caching of Dereference ops on ref-counted columns,
//! 4) counting how many dereferences are required for `get` to return None.

use parity_db::{Db, Operation, Options};
use std::path::{Path, PathBuf};

const KEY: [u8; 32] = [0x42; 32];
const DATA: &[u8] = b"hello refcount investigation payload";

fn make_options(path: &Path) -> Options {
	let mut opts = Options::with_columns(path, 1);
	opts.columns[0].ref_counted = true;
	opts.columns[0].preimage = true;
	opts.columns[0].uniform = true;
	opts
}

fn open_db(path: PathBuf) -> Db {
	Db::open_or_create(&make_options(&path)).expect("paritydb open")
}

fn close_and_reopen(db: Db, path: &Path) -> Db {
	drop(db);
	Db::open_or_create(&make_options(path)).expect("paritydb reopen")
}

fn measure_refcount(mut db: Db, path: &Path) -> u32 {
	db = close_and_reopen(db, path);
	let mut count = 0u32;
	loop {
		if db.get(0, &KEY).expect("get").is_none() {
			return count;
		}
		db.commit(std::iter::once((0u8, KEY.to_vec(), None::<Vec<u8>>)))
			.expect("dereference commit");
		db = close_and_reopen(db, path);
		count += 1;
		if count > 4096 {
			panic!("runaway refcount, gave up at 4096");
		}
	}
}

#[test]
fn paritydb_baseline_single_set_should_be_refcount_1() {
	let dir = tempfile::TempDir::new().unwrap();
	let db = open_db(dir.path().to_owned());

	db.commit(std::iter::once((0u8, KEY.to_vec(), Some(DATA.to_vec()))))
		.expect("initial set");

	let actual = measure_refcount(db, dir.path());
	println!("paritydb_baseline_single_set_should_be_refcount_1: expected = 1, actual = {}", actual);
}

#[test]
fn paritydb_baseline_two_separate_sets_should_be_refcount_2() {
	let dir = tempfile::TempDir::new().unwrap();
	let db = open_db(dir.path().to_owned());

	db.commit(std::iter::once((0u8, KEY.to_vec(), Some(DATA.to_vec()))))
		.expect("first set");
	db.commit(std::iter::once((0u8, KEY.to_vec(), Some(DATA.to_vec()))))
		.expect("second set");

	let actual = measure_refcount(db, dir.path());
	println!("paritydb_baseline_two_separate_sets_should_be_refcount_2: expected = 2, actual = {}", actual);
}

#[test]
fn paritydb_three_tuple_set_inc_ref_pattern() {
	let dir = tempfile::TempDir::new().unwrap();
	let db = open_db(dir.path().to_owned());

	db.commit(std::iter::once((0u8, KEY.to_vec(), Some(DATA.to_vec()))))
		.expect("initial set");

	let bumps = vec![
		(0u8, KEY.to_vec(), Some(DATA.to_vec())),
		(0u8, KEY.to_vec(), Some(DATA.to_vec())),
		(0u8, KEY.to_vec(), Some(DATA.to_vec())),
		(0u8, KEY.to_vec(), Some(DATA.to_vec())),
		(0u8, KEY.to_vec(), Some(DATA.to_vec())),
	];
	db.commit(bumps).expect("five bumps via Some(value) tuples");

	let actual = measure_refcount(db, dir.path());
	println!(
		"paritydb_three_tuple_set_inc_ref_pattern: \
		expected refcount = 6 (1 initial + 5 bumps), actual = {}",
		actual,
	);
}

#[test]
fn paritydb_native_reference_op_pattern() {
	let dir = tempfile::TempDir::new().unwrap();
	let db = open_db(dir.path().to_owned());

	db.commit(std::iter::once((0u8, KEY.to_vec(), Some(DATA.to_vec()))))
		.expect("initial set");

	let bumps: Vec<(u8, Operation<Vec<u8>, Vec<u8>>)> = vec![
		(0, Operation::Reference(KEY.to_vec())),
		(0, Operation::Reference(KEY.to_vec())),
		(0, Operation::Reference(KEY.to_vec())),
		(0, Operation::Reference(KEY.to_vec())),
		(0, Operation::Reference(KEY.to_vec())),
	];
	db.commit_changes(bumps).expect("five bumps via Operation::Reference");

	let actual = measure_refcount(db, dir.path());
	println!(
		"paritydb_native_reference_op_pattern: \
		expected refcount = 6 (1 initial + 5 bumps), actual = {}",
		actual,
	);
}

#[test]
fn paritydb_one_bump_per_commit() {
	let dir = tempfile::TempDir::new().unwrap();
	let db = open_db(dir.path().to_owned());

	db.commit(std::iter::once((0u8, KEY.to_vec(), Some(DATA.to_vec()))))
		.expect("initial set");

	for _ in 0..5 {
		db.commit(std::iter::once((0u8, KEY.to_vec(), Some(DATA.to_vec()))))
			.expect("single bump commit");
	}

	let actual = measure_refcount(db, dir.path());
	println!(
		"paritydb_one_bump_per_commit: \
		expected refcount = 6 (1 initial + 5 separate bump commits), actual = {}",
		actual,
	);
}

#[test]
fn paritydb_one_native_reference_per_commit() {
	let dir = tempfile::TempDir::new().unwrap();
	let db = open_db(dir.path().to_owned());

	db.commit(std::iter::once((0u8, KEY.to_vec(), Some(DATA.to_vec()))))
		.expect("initial set");

	for _ in 0..5 {
		db.commit_changes(std::iter::once((0u8, Operation::Reference(KEY.to_vec()))))
			.expect("single native reference commit");
	}

	let actual = measure_refcount(db, dir.path());
	println!(
		"paritydb_one_native_reference_per_commit: \
		expected refcount = 6 (1 initial + 5 separate Reference commits), actual = {}",
		actual,
	);
}

#[test]
fn paritydb_reference_on_missing_key_silently_drops_bump() {
	let dir = tempfile::TempDir::new().unwrap();
	let db = open_db(dir.path().to_owned());

	for _ in 0..5 {
		db.commit_changes(std::iter::once((0u8, Operation::Reference(KEY.to_vec()))))
			.expect("Reference on missing key");
	}

	db.commit(std::iter::once((0u8, KEY.to_vec(), Some(DATA.to_vec()))))
		.expect("set after the missing references");

	let actual = measure_refcount(db, dir.path());
	println!(
		"paritydb_reference_on_missing_key_silently_drops_bump: \
		this is the live-block-renew-during-warp-sync scenario; \
		5 Renew(X) on missing X, then Insert(X); expected refcount if Reference-on-missing \
		were honored = 6, but parity-db drops them: actual = {}",
		actual,
	);
}

#[test]
fn substrate_pattern_store_plus_multi_reference_in_one_transaction() {
	let dir = tempfile::TempDir::new().unwrap();
	let db = open_db(dir.path().to_owned());

	db.commit(vec![
		(0u8, KEY.to_vec(), Some(DATA.to_vec())),
		(0u8, KEY.to_vec(), None::<Vec<u8>>),
		(0u8, KEY.to_vec(), None::<Vec<u8>>),
		(0u8, KEY.to_vec(), None::<Vec<u8>>),
		(0u8, KEY.to_vec(), None::<Vec<u8>>),
	])
	.expect("substrate-equivalent: 1 Store + 4 References on missing key");

	let actual = measure_refcount(db, dir.path());
	println!(
		"substrate_pattern_store_plus_multi_reference_in_one_transaction: \
		this replays exactly what substrate's parity_db DbAdapter emits for \
		Transaction::[store(X,data), reference(X), reference(X), reference(X), reference(X)]; \
		each subsequent Reference reads X-not-yet-in-overlay -> emits Dereference tuple; \
		expected if substrate Reference were honored = 5, actual = {}",
		actual,
	);
}

#[test]
fn paritydb_set_different_value_on_existing_key() {
	let dir = tempfile::TempDir::new().unwrap();
	let db = open_db(dir.path().to_owned());

	let original = b"original-payload-XXXXXXXXXXXXXXXXXXXXX".to_vec();
	let replacement = b"replacement-payload-YYYYYYYYYYYYYYYY".to_vec();
	assert_ne!(original, replacement);
	assert_eq!(original.len(), replacement.len(), "same length to isolate the value-content question");

	db.commit(std::iter::once((0u8, KEY.to_vec(), Some(original.clone()))))
		.expect("initial set with original");

	db.commit(std::iter::once((0u8, KEY.to_vec(), Some(replacement.clone()))))
		.expect("set with replacement value on same key");

	drop(db);
	let db = open_db(dir.path().to_owned());

	let stored = db.get(0, &KEY).expect("get").expect("entry must exist");
	let stored_eq_original = stored == original;
	let stored_eq_replacement = stored == replacement;

	let refcount = measure_refcount(db, dir.path());

	println!(
		"paritydb_set_different_value_on_existing_key: \
		stored == original: {}, stored == replacement: {}, refcount = {}",
		stored_eq_original, stored_eq_replacement, refcount,
	);
}

#[test]
fn paritydb_set_empty_then_real_value_on_missing_key() {
	let dir = tempfile::TempDir::new().unwrap();
	let db = open_db(dir.path().to_owned());

	db.commit(std::iter::once((0u8, KEY.to_vec(), Some(Vec::<u8>::new()))))
		.expect("set empty placeholder on missing key");

	db.commit(std::iter::once((0u8, KEY.to_vec(), Some(DATA.to_vec()))))
		.expect("set real data on existing empty placeholder");

	drop(db);
	let db = open_db(dir.path().to_owned());
	let stored = db.get(0, &KEY).expect("get").expect("entry must exist");
	let stored_is_empty = stored.is_empty();
	let stored_is_real = stored == DATA;

	let refcount = measure_refcount(db, dir.path());

	println!(
		"paritydb_set_empty_then_real_value_on_missing_key: \
		stored.is_empty: {}, stored == DATA: {}, refcount = {}",
		stored_is_empty, stored_is_real, refcount,
	);
}

#[test]
fn paritydb_three_tuple_some_value_on_missing_key_creates_with_rc1() {
	let dir = tempfile::TempDir::new().unwrap();
	let db = open_db(dir.path().to_owned());

	for _ in 0..5 {
		db.commit(std::iter::once((0u8, KEY.to_vec(), Some(DATA.to_vec()))))
			.expect("Some(value) on missing then existing key");
	}

	let actual = measure_refcount(db, dir.path());
	println!(
		"paritydb_three_tuple_some_value_on_missing_key_creates_with_rc1: \
		5 Some(value) commits on initially-missing key; first creates with rc=1, \
		subsequent 4 inc_ref; expected = 5, actual = {}",
		actual,
	);
}
