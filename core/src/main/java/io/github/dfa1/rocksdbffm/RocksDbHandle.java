package io.github.dfa1.rocksdbffm;

import java.lang.foreign.MemorySegment;

/// Marker for types backed by a native `rocksdb_t*` pointer.
///
/// All permitted types expose a `rocksdb_t*` via [#ptr()]:
/// - [ReadWriteDB], [TtlDB], [ReadOnlyDB], [SecondaryDB] — opened directly as `rocksdb_t*`
/// - [TransactionDB] — base `rocksdb_t*` via `rocksdb_transactiondb_get_base_db`
/// - [OptimisticTransactionDB] — base `rocksdb_t*` via `rocksdb_optimistictransactiondb_get_base_db`
public sealed interface RocksDbHandle permits ReadWriteDB, TtlDB, ReadOnlyDB, SecondaryDB, TransactionDB, OptimisticTransactionDB {
	MemorySegment ptr();
}
