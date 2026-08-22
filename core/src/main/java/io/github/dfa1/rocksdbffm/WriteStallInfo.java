package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// Read-only view of a write-controller state change, passed to
/// [EventNotifier#onStallConditionsChanged(WriteStallInfo)].
///
/// Wraps a `const rocksdb_writestallinfo_t*` owned by RocksDB for the duration of the callback
/// only: every accessor reads through that pointer on demand, so an instance must never be
/// retained or used after the callback method returns.
public final class WriteStallInfo {

	/// `const char* rocksdb_writestallinfo_cf_name(const rocksdb_writestallinfo_t*, size_t* size);`
	private static final MethodHandle MH_CF_NAME = NativeLibrary.lookup(
			"rocksdb_writestallinfo_cf_name",
			FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

	/// `const rocksdb_writestallcondition_t* rocksdb_writestallinfo_cur(const rocksdb_writestallinfo_t*);`
	private static final MethodHandle MH_CUR = NativeLibrary.lookup(
			"rocksdb_writestallinfo_cur", FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

	/// `const rocksdb_writestallcondition_t* rocksdb_writestallinfo_prev(const rocksdb_writestallinfo_t*);`
	private static final MethodHandle MH_PREV = NativeLibrary.lookup(
			"rocksdb_writestallinfo_prev", FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

	private final MemorySegment ptr;

	WriteStallInfo(MemorySegment ptr) {
		this.ptr = ptr;
	}

	/// Returns the name of the column family whose write stall state changed.
	///
	/// @return the column family name
	public String columnFamilyName() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment sizeHolder = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment namePtr = (MemorySegment) MH_CF_NAME.invokeExact(ptr, sizeHolder);
			return RocksDB.toJavaString(namePtr, sizeHolder.get(ValueLayout.JAVA_LONG, 0));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("WriteStallInfo.columnFamilyName failed", t);
		}
	}

	/// Returns the new write stall condition.
	///
	/// @return the current write stall condition
	public WriteStallCondition current() {
		try {
			MemorySegment condPtr = (MemorySegment) MH_CUR.invokeExact(ptr);
			return WriteStallCondition.fromValue(condPtr.reinterpret(ValueLayout.JAVA_INT.byteSize())
					.get(ValueLayout.JAVA_INT, 0));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("WriteStallInfo.current failed", t);
		}
	}

	/// Returns the write stall condition in effect immediately before this change.
	///
	/// @return the previous write stall condition
	public WriteStallCondition previous() {
		try {
			MemorySegment condPtr = (MemorySegment) MH_PREV.invokeExact(ptr);
			return WriteStallCondition.fromValue(condPtr.reinterpret(ValueLayout.JAVA_INT.byteSize())
					.get(ValueLayout.JAVA_INT, 0));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("WriteStallInfo.previous failed", t);
		}
	}
}
