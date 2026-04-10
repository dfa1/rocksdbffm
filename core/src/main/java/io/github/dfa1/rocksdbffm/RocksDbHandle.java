package io.github.dfa1.rocksdbffm;

import java.lang.foreign.MemorySegment;

/// Marker for types backed by a native `rocksdb_t*` pointer.
///
/// Sealed to the four types that open via the standard `rocksdb_t*` C API:
/// [ReadWriteDB], [TtlDB], [ReadOnlyDB], and [SecondaryDB].
///
/// [TransactionDB] and [OptimisticTransactionDB] are deliberately excluded —
/// they use distinct C pointer types (`rocksdb_transactiondb_t*` and
/// `rocksdb_optimistictransactiondb_t*`) and a separate function namespace.
///
/// This interface exists primarily to let utilities like [Checkpoint] accept
/// any `rocksdb_t*`-backed DB in a single method, rather than one overload
/// per concrete type.
public sealed interface RocksDbHandle permits ReadWriteDB, TtlDB, ReadOnlyDB, SecondaryDB {
	MemorySegment ptr();
}
