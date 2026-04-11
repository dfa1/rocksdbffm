package io.github.dfa1.rocksdbffm;

import java.lang.foreign.MemorySegment;

/// Marker for types backed by a native `rocksdb_t*` pointer.
///
/// Sealed to the four types that open via the standard `rocksdb_t*` C API:
/// [ReadWriteDB], [TtlDB], [ReadOnlyDB], and [SecondaryDB].
///
/// [TransactionDB] and [OptimisticTransactionDB] are deliberately excluded —
/// they use distinct C pointer types (`rocksdb_transactiondb_t*` and
/// `rocksdb_optimistictransactiondb_t*`) and a separate function namespace. ->
/// TODO: use rocksdb_t* rocksdb_optimistictransactiondb_get_base_db(
///     rocksdb_optimistictransactiondb_t* txn_db);
public sealed interface RocksDbHandle permits ReadWriteDB, TtlDB, ReadOnlyDB, SecondaryDB {
	MemorySegment ptr();
}
