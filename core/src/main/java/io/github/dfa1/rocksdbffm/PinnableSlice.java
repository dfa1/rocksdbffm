package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_pinnableslice_t` — the pinned-value handle returned by the
/// byte[]-tier `rocksdb_*get_pinned[_cf]` family (`rocksdb_get_pinned[_cf]`,
/// `rocksdb_transaction_get_pinned[_cf]`, `rocksdb_transactiondb_get_pinned[_cf]`).
///
/// Not the same native type as `rocksdb_pinnable_handle_t` (see `RocksDB.withPinned`),
/// which is the newer, zero-copy `_v2` handle. `Transaction` and `TransactionDB` have no
/// `_v2` equivalent in `rocksdb/include/rocksdb/c.h`, so they still go through this
/// older API.
///
/// Package-private: purely internal plumbing for the `get_pinned`-based `get(...)`
/// overloads on [RocksDB], [Transaction], and [TransactionDB] — never returned to callers.
/// This is the single mapping of `rocksdb_pinnableslice_value`/`_destroy`; those three
/// classes used to each map the same two symbols independently.
final class PinnableSlice extends NativeObject {

	/// `const char* rocksdb_pinnableslice_value(const rocksdb_pinnableslice_t* t, size_t* vlen);`
	private static final MethodHandle MH_VALUE;
	/// `void rocksdb_pinnableslice_destroy(rocksdb_pinnableslice_t* v);`
	private static final MethodHandle MH_DESTROY;

	static {
		MH_VALUE = NativeLibrary.lookup("rocksdb_pinnableslice_value",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_DESTROY = NativeLibrary.lookup("rocksdb_pinnableslice_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
	}

	private PinnableSlice(MemorySegment ptr) {
		super(ptr);
	}

	/// Wraps `ptr`, or returns `null` if `ptr` is `MemorySegment.NULL` — the
	/// `get_pinned`/`get_pinned_cf` downcalls return NULL for NotFound or error (the two
	/// are distinguished only by `errptr`, which callers must check separately).
	///
	/// @param ptr the raw `rocksdb_pinnableslice_t*`, or `MemorySegment.NULL`
	/// @return a wrapper owning `ptr`, or `null` if `ptr` is `MemorySegment.NULL`
	static PinnableSlice wrapOrNull(MemorySegment ptr) {
		return MemorySegment.NULL.equals(ptr) ? null : new PinnableSlice(ptr);
	}

	/// Returns the raw value pointer and writes its length into `vallenOut`, a
	/// caller-allocated `size_t*` slot.
	///
	/// @param vallenOut native slot to receive the value's length
	/// @return pointer to the value's bytes
	MemorySegment value(MemorySegment vallenOut) {
		try {
			return (MemorySegment) MH_VALUE.invokeExact(ptr(), vallenOut);
		} catch (Throwable t) {
			throw RocksDBException.wrap("pinnableslice value failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
