use clap::Parser;
use codec::{Compact, CompactLen, Decode};
use sp_database::Database;
use std::path::PathBuf;

const NUM_COLUMNS: u32 = 13;

mod columns {
	pub const META: u32 = 0;
	pub const KEY_LOOKUP: u32 = 3;
	pub const HEADER: u32 = 4;
	pub const BODY: u32 = 5;
	pub const BODY_INDEX: u32 = 12;
	pub const TRANSACTION: u32 = 11;
}

mod meta_keys {
	pub const BEST_BLOCK: &[u8; 4] = b"best";
	pub const FINALIZED_BLOCK: &[u8; 5] = b"final";
	pub const GENESIS_HASH: &[u8; 3] = b"gen";
}

type DbHash = sp_core::H256;

#[allow(dead_code)]
#[derive(Debug, Decode)]
enum DbExtrinsic {
	Indexed { hash: DbHash, header: Vec<u8> },
	Full(Vec<u8>),
}

#[derive(Parser)]
#[command(about = "Inspect a substrate storage chain database")]
struct Cli {
	#[arg(long)]
	database: PathBuf,

	#[arg(long, help = "Show blocks in range, e.g. 150..200")]
	range: Option<String>,

	#[arg(long, help = "Inspect a single block in detail")]
	block: Option<u32>,

	#[arg(long, help = "Show header-only blocks (no body/body_index)")]
	show_header_only: bool,

	#[arg(long, help = "Verify all BODY_INDEX content_hashes exist in TRANSACTION column")]
	check: bool,
}

fn number_index_key(n: u32) -> [u8; 4] {
	[(n >> 24) as u8, ((n >> 16) & 0xff) as u8, ((n >> 8) & 0xff) as u8, (n & 0xff) as u8]
}

fn hex(bytes: &[u8]) -> String {
	bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn main() {
	let cli = Cli::parse();

	let db_path = if cli.database.join("data/chains").exists() {
		find_db_path(&cli.database)
	} else {
		cli.database.clone()
	};

	eprintln!("Opening database at: {}", db_path.display());

	let mut db_config = kvdb_rocksdb::DatabaseConfig::with_columns(NUM_COLUMNS);
	db_config.create_if_missing = false;
	let rocksdb =
		kvdb_rocksdb::Database::open(&db_config, &db_path).expect("Failed to open database");
	let db = sp_database::as_rocksdb_database(rocksdb);
	let db = &*db;

	print_meta(db);
	println!();

	if cli.check {
		check_integrity(db);
		return;
	}

	if let Some(block_num) = cli.block {
		inspect_block(db, block_num);
		return;
	}

	let best = get_best_block_number(db);
	let (from, to) = parse_range(&cli.range, best);
	eprintln!("Scanning blocks {from}..={to}");
	println!();

	println!(
		"{:>7}  {:>66}  {:>6}  {:>6}  {:>10}  {:>8}",
		"Block", "Hash", "Header", "Body", "BodyIndex", "IndexTx"
	);
	println!("{}", "-".repeat(114));

	for n in from..=to {
		print_block(db, n, cli.show_header_only);
	}
}

fn parse_range(range_str: &Option<String>, best: u32) -> (u32, u32) {
	match range_str {
		Some(s) => {
			let parts: Vec<&str> = s.split("..").collect();
			match parts.as_slice() {
				[from, to] => {
					let from = from.parse::<u32>().unwrap_or(0);
					let to = to.parse::<u32>().unwrap_or(best);
					(from, to)
				},
				_ => {
					eprintln!("Invalid range format '{}', expected FROM..TO", s);
					(0, best)
				},
			}
		},
		None => (0, best),
	}
}

fn find_db_path(base: &PathBuf) -> PathBuf {
	let chains_dir = base.join("data/chains");
	if let Ok(entries) = std::fs::read_dir(&chains_dir) {
		for entry in entries.flatten() {
			let full_path = entry.path().join("db/full");
			if full_path.exists() {
				return full_path;
			}
		}
	}
	base.clone()
}

fn print_meta(db: &dyn Database<DbHash>) {
	match db.get(columns::META, meta_keys::GENESIS_HASH) {
		Some(genesis) => println!("Genesis hash:    0x{}", hex(&genesis)),
		None => println!("Genesis hash:    <missing>"),
	}

	match db.get(columns::META, meta_keys::BEST_BLOCK) {
		Some(best_key) => match db.get(columns::HEADER, &best_key) {
			Some(header_bytes) => {
				let (number, hash) = decode_header_number_hash(&header_bytes);
				println!("Best block:      #{number} (0x{hash})");
			},
			None => println!("Best block:      <header missing, key=0x{}>", hex(&best_key)),
		},
		None => println!("Best block:      <meta missing>"),
	}

	match db.get(columns::META, meta_keys::FINALIZED_BLOCK) {
		Some(fin_key) => match db.get(columns::HEADER, &fin_key) {
			Some(header_bytes) => {
				let (number, hash) = decode_header_number_hash(&header_bytes);
				println!("Finalized block: #{number} (0x{hash})");
			},
			None => println!("Finalized block: <header missing, key=0x{}>", hex(&fin_key)),
		},
		None => println!("Finalized block: <meta missing>"),
	}
}

fn number_from_lookup_key(lookup_key: &[u8]) -> Option<u32> {
	if lookup_key.len() >= 4 {
		Some(
			(lookup_key[0] as u32) << 24 |
				(lookup_key[1] as u32) << 16 |
				(lookup_key[2] as u32) << 8 |
				(lookup_key[3] as u32),
		)
	} else {
		None
	}
}

fn get_best_block_number(db: &dyn Database<DbHash>) -> u32 {
	if let Some(best_key) = db.get(columns::META, meta_keys::BEST_BLOCK) {
		if let Some(header_bytes) = db.get(columns::HEADER, &best_key) {
			let (number, _) = decode_header_number_hash(&header_bytes);
			return number;
		}
		if let Some(n) = number_from_lookup_key(&best_key) {
			eprintln!("WARNING: best block header missing, using number from lookup key");
			return n;
		}
		eprintln!("WARNING: best block meta exists but header and key are unreadable");
	} else {
		eprintln!("WARNING: no best block meta found, scanning only genesis");
	}
	0
}

fn decode_header_number_hash(header_bytes: &[u8]) -> (u32, String) {
	let hash = sp_core::hashing::blake2_256(header_bytes);
	let mut input = &header_bytes[..];
	// Header: parent_hash(32) + number(compact) + state_root(32) + extrinsics_root(32) + digest
	let _parent_hash = <[u8; 32]>::decode(&mut input).unwrap_or_default();
	let number = <Compact<u32>>::decode(&mut input).map(|c| c.0).unwrap_or(0);
	(number, hex(&hash))
}

fn hash_from_lookup_key(lookup_key: &[u8]) -> Option<String> {
	if lookup_key.len() > 4 {
		Some(format!("0x{}", hex(&lookup_key[4..])))
	} else {
		None
	}
}

fn has_body(db: &dyn Database<DbHash>, lookup_key: &[u8]) -> bool {
	db.get(columns::BODY, lookup_key).is_some() || db.get(columns::BODY_INDEX, lookup_key).is_some()
}

fn print_block(db: &dyn Database<DbHash>, n: u32, show_header_only: bool) {
	let key = number_index_key(n);
	let lookup_key = match db.get(columns::KEY_LOOKUP, &key) {
		Some(k) => k,
		None => return,
	};

	if !show_header_only && !has_body(db, &lookup_key) {
		return;
	}

	let header_bytes = db.get(columns::HEADER, &lookup_key);
	let body_bytes = db.get(columns::BODY, &lookup_key);
	let body_index_bytes = db.get(columns::BODY_INDEX, &lookup_key);

	let hash_str = match &header_bytes {
		Some(h) => {
			let (_, hash) = decode_header_number_hash(h);
			format!("0x{hash}")
		},
		None => hash_from_lookup_key(&lookup_key).unwrap_or_else(|| "missing".to_string()),
	};

	let header_status = if header_bytes.is_some() { "yes" } else { "MISS" };

	let body_status = if body_bytes.is_some() {
		"plain"
	} else if body_index_bytes.is_some() {
		"index"
	} else {
		"MISS"
	};

	let (index_count_str, indexed_tx) = match &body_index_bytes {
		Some(index_raw) => match Vec::<DbExtrinsic>::decode(&mut &index_raw[..]) {
			Ok(entries) => {
				let total = entries.len();
				let indexed =
					entries.iter().filter(|e| matches!(e, DbExtrinsic::Indexed { .. })).count();
				(format!("{total}"), format!("{indexed}"))
			},
			Err(_) => ("err".to_string(), "err".to_string()),
		},
		None if body_bytes.is_some() => ("-".to_string(), "0".to_string()),
		None => ("-".to_string(), "-".to_string()),
	};

	println!(
		"{n:>7}  {hash_str:>66}  {header_status:>6}  {body_status:>6}  {index_count_str:>10}  {indexed_tx:>8}"
	);
}

fn is_store(header: &[u8]) -> bool {
	match extrinsic_body_length(header) {
		Some(body_len) => {
			let compact_overhead = Compact::<u32>::compact_len(&body_len);
			let full_ext_len = compact_overhead + body_len as usize;
			full_ext_len > header.len()
		},
		None => false,
	}
}

fn check_integrity(db: &dyn Database<DbHash>) {
	let best = get_best_block_number(db);
	println!("Checking blocks 0..={best}");
	println!();

	let mut blocks_checked = 0u32;
	let mut blocks_with_body = 0u32;
	let mut blocks_with_index = 0u32;
	let mut indexed_total = 0u32;
	let mut stores = 0u32;
	let mut renews = 0u32;
	let mut missing_tx = 0u32;
	let mut errors: Vec<String> = Vec::new();

	for n in 0..=best {
		let key = number_index_key(n);
		let lookup_key = match db.get(columns::KEY_LOOKUP, &key) {
			Some(k) => k,
			None => continue,
		};

		blocks_checked += 1;

		if db.get(columns::BODY, &lookup_key).is_some() {
			blocks_with_body += 1;
		}

		let index_raw = match db.get(columns::BODY_INDEX, &lookup_key) {
			Some(raw) => raw,
			None => continue,
		};

		blocks_with_index += 1;

		let entries = match Vec::<DbExtrinsic>::decode(&mut &index_raw[..]) {
			Ok(e) => e,
			Err(e) => {
				errors.push(format!("Block #{n}: failed to decode BODY_INDEX: {e}"));
				continue;
			},
		};

		for (i, ex) in entries.iter().enumerate() {
			if let DbExtrinsic::Indexed { hash, header } = ex {
				indexed_total += 1;
				let is_store_tx = is_store(header);
				if is_store_tx {
					stores += 1;
				} else {
					renews += 1;
				}

				let kind = if is_store_tx { "STORE" } else { "RENEW" };

				match db.get(columns::TRANSACTION, hash.as_ref()) {
					Some(data) => {
						if is_store_tx {
							let computed = sp_core::hashing::blake2_256(&data);
							if computed != hash.as_ref() {
								errors.push(format!(
									"Block #{n} ext[{i}] {kind}: hash mismatch! expected 0x{}, got 0x{}",
									hex(hash.as_ref()),
									hex(&computed),
								));
							}
						}
					},
					None => {
						missing_tx += 1;
						errors.push(format!(
							"Block #{n} ext[{i}] {kind}: TRANSACTION data MISSING for hash 0x{}",
							hex(hash.as_ref()),
						));
					},
				}
			}
		}
	}

	println!("Summary:");
	println!("  Blocks scanned:     {blocks_checked}");
	println!("  Blocks with BODY:   {blocks_with_body}");
	println!("  Blocks with INDEX:  {blocks_with_index}");
	println!("  Indexed extrinsics: {indexed_total} ({stores} store, {renews} renew)");
	println!("  Missing tx data:    {missing_tx}");
	println!();

	if errors.is_empty() {
		println!("✓ All indexed transaction hashes are present in TRANSACTION column");
	} else {
		println!("✗ Found {} error(s):", errors.len());
		for e in &errors {
			println!("  {e}");
		}
		std::process::exit(1);
	}
}

fn extrinsic_body_length(header: &[u8]) -> Option<u32> {
	let mut input = &header[..];
	let len = <Compact<u32>>::decode(&mut input).ok()?.0;
	Some(len)
}

fn inspect_block(db: &dyn Database<DbHash>, n: u32) {
	let key = number_index_key(n);
	let lookup_key = match db.get(columns::KEY_LOOKUP, &key) {
		Some(k) => k,
		None => {
			println!("Block #{n}: no lookup key found");
			return;
		},
	};

	let header_bytes = db.get(columns::HEADER, &lookup_key);
	let hash_str = match &header_bytes {
		Some(h) => {
			let (_, hash) = decode_header_number_hash(h);
			format!("0x{hash}")
		},
		None => hash_from_lookup_key(&lookup_key).unwrap_or_else(|| "?".to_string()),
	};

	println!("Block #{n}");
	println!("  Hash:   {hash_str}");
	println!("  Header: {}", if header_bytes.is_some() { "present" } else { "MISSING" });

	let body_bytes = db.get(columns::BODY, &lookup_key);
	let body_index_bytes = db.get(columns::BODY_INDEX, &lookup_key);

	match (&body_bytes, &body_index_bytes) {
		(Some(_), _) => println!("  Body:   plain (stored as BODY column)"),
		(None, Some(_)) => println!("  Body:   indexed (stored as BODY_INDEX column)"),
		(None, None) => {
			println!("  Body:   MISSING (pruned)");
			return;
		},
	}

	if let Some(index_raw) = &body_index_bytes {
		match Vec::<DbExtrinsic>::decode(&mut &index_raw[..]) {
			Ok(entries) => {
				println!("  Extrinsics: {} total", entries.len());
				println!();
				for (i, ex) in entries.iter().enumerate() {
					match ex {
						DbExtrinsic::Indexed { hash, header } => {
							let tx_data = db.get(columns::TRANSACTION, hash.as_ref());
							let tx_len = tx_data.as_ref().map(|d| d.len()).unwrap_or(0);

							let kind = match extrinsic_body_length(header) {
								Some(body_len) => {
									let compact_overhead = Compact::<u32>::compact_len(&body_len);
									let full_ext_len = compact_overhead + body_len as usize;
									if full_ext_len > header.len() {
										"STORE"
									} else {
										"RENEW"
									}
								},
								None => "UNKNOWN",
							};

							println!("  [{i}] {kind}");
							println!("       content_hash: 0x{}", hex(hash.as_ref()));
							println!("       header_len:   {} bytes", header.len());
							println!(
								"       tx_data:      {} bytes{}",
								tx_len,
								if tx_data.is_none() { " (MISSING)" } else { "" }
							);
						},
						DbExtrinsic::Full(_) => {
							println!("  [{i}] FULL (not indexed)");
						},
					}
				}
			},
			Err(e) => println!("  Error decoding BODY_INDEX: {e}"),
		}
	}
}
