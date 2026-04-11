package io.github.dfa1.rocksdbffm;

import java.lang.foreign.MemorySegment;

/// Marker for types backed by a native `rocksdb_t*` pointer.
///
/// Sealed to the types whose [#ptr()] returns a `rocksdb_t*`:
/// [ReadWriteDB], [TtlDB], [ReadOnlyDB], [SecondaryDB], and
/// [OptimisticTransactionDB] (which exposes its base `rocksdb_t*` obtained
/// via `rocksdb_optimistictransactiondb_get_base_db`).
///
/// [TransactionDB] is excluded — it uses `rocksdb_transactiondb_t*` with a
/// separate function namespace and no shared `rocksdb_t*` helpers.
public sealed interface RocksDbHandle permits ReadWriteDB, TtlDB, ReadOnlyDB, SecondaryDB, OptimisticTransactionDB {
	MemorySegment ptr();
}
