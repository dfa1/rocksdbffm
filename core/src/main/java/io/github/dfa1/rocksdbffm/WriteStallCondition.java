package io.github.dfa1.rocksdbffm;

/// State of RocksDB's write controller, reported on [WriteStallInfo#current()] and
/// [WriteStallInfo#previous()].
///
/// Integer values match `rocksdb::WriteStallCondition` (`rocksdb/types.h`).
///
/// Unlike every other enum in this package, there is no `rocksdb_*` getter that hands back this
/// value as a plain `int`: `rocksdb_writestallinfo_cur()`/`_prev()` return a
/// `const rocksdb_writestallcondition_t*`, and per `db/c.cc` that pointer is a bare
/// `reinterpret_cast` of the underlying `WriteStallCondition` field — `struct
/// rocksdb_writestallcondition_t { WriteStallCondition rep; }`, with `rep` as its only member at
/// offset zero. Since a C++ scoped enum with no explicit underlying type defaults to `int`,
/// [WriteStallInfo] reads that value directly as a 4-byte int rather than through a getter
/// function, relying on the vendored `c.cc` source rather than a published API contract.
public enum WriteStallCondition {

	/// Writes are being delayed (slowed down) but not stopped.
	DELAYED,

	/// Writes are stopped entirely.
	STOPPED,

	/// No write stall in effect.
	NORMAL;

	static WriteStallCondition fromValue(int value) {
		return switch (value) {
			case 0 -> DELAYED;
			case 1 -> STOPPED;
			case 2 -> NORMAL;
			default -> throw new IllegalArgumentException("Unknown write stall condition value: " + value);
		};
	}
}
