package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// Read-only view of a memtable that just became immutable, passed to
/// [EventNotifier#onMemTableSealed(MemTableInfo)].
///
/// Wraps a `const rocksdb_memtableinfo_t*` owned by RocksDB for the duration of the callback
/// only: every accessor reads through that pointer on demand, so an instance must never be
/// retained or used after the callback method returns.
public final class MemTableInfo {

	/// `const char* rocksdb_memtableinfo_cf_name(const rocksdb_memtableinfo_t*, size_t* size);`
	private static final MethodHandle MH_CF_NAME = NativeLibrary.lookup(
			"rocksdb_memtableinfo_cf_name",
			FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

	/// `uint64_t rocksdb_memtableinfo_first_seqno(const rocksdb_memtableinfo_t*);`
	private static final MethodHandle MH_FIRST_SEQNO = NativeLibrary.lookup(
			"rocksdb_memtableinfo_first_seqno", FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

	/// `uint64_t rocksdb_memtableinfo_earliest_seqno(const rocksdb_memtableinfo_t*);`
	private static final MethodHandle MH_EARLIEST_SEQNO = NativeLibrary.lookup(
			"rocksdb_memtableinfo_earliest_seqno", FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

	/// `uint64_t rocksdb_memtableinfo_num_entries(const rocksdb_memtableinfo_t*);`
	private static final MethodHandle MH_NUM_ENTRIES = NativeLibrary.lookup(
			"rocksdb_memtableinfo_num_entries", FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

	/// `uint64_t rocksdb_memtableinfo_num_deletes(const rocksdb_memtableinfo_t*);`
	private static final MethodHandle MH_NUM_DELETES = NativeLibrary.lookup(
			"rocksdb_memtableinfo_num_deletes", FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

	/// `const char* rocksdb_memtableinfo_newest_udt(const rocksdb_memtableinfo_t*, size_t* size);`
	private static final MethodHandle MH_NEWEST_UDT = NativeLibrary.lookup(
			"rocksdb_memtableinfo_newest_udt",
			FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

	private final MemorySegment ptr;

	MemTableInfo(MemorySegment ptr) {
		this.ptr = ptr;
	}

	/// Returns the name of the column family the sealed memtable belonged to.
	///
	/// @return the column family name
	public String columnFamilyName() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment sizeHolder = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment namePtr = (MemorySegment) MH_CF_NAME.invokeExact(ptr, sizeHolder);
			return RocksDB.toJavaString(namePtr, sizeHolder.get(ValueLayout.JAVA_LONG, 0));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("MemTableInfo.columnFamilyName failed", t);
		}
	}

	/// Returns the sequence number of the first entry written to this memtable.
	///
	/// @return the first sequence number
	public SequenceNumber firstSequenceNumber() {
		try {
			return SequenceNumber.of((long) MH_FIRST_SEQNO.invokeExact(ptr));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("MemTableInfo.firstSequenceNumber failed", t);
		}
	}

	/// Returns the earliest sequence number still readable through this memtable, accounting for
	/// any snapshots that keep older entries alive.
	///
	/// @return the earliest sequence number
	public SequenceNumber earliestSequenceNumber() {
		try {
			return SequenceNumber.of((long) MH_EARLIEST_SEQNO.invokeExact(ptr));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("MemTableInfo.earliestSequenceNumber failed", t);
		}
	}

	/// Returns the total number of entries (puts, merges, and deletes) in this memtable.
	///
	/// @return the number of entries
	public long numEntries() {
		try {
			return (long) MH_NUM_ENTRIES.invokeExact(ptr);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("MemTableInfo.numEntries failed", t);
		}
	}

	/// Returns the number of delete entries in this memtable.
	///
	/// @return the number of deletes
	public long numDeletes() {
		try {
			return (long) MH_NUM_DELETES.invokeExact(ptr);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("MemTableInfo.numDeletes failed", t);
		}
	}

	/// Returns the newest user-defined timestamp stored in this memtable, if user-defined
	/// timestamps are enabled for this column family.
	///
	/// @return the newest user-defined timestamp, as raw bytes; empty if none
	public byte[] newestUserDefinedTimestamp() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment sizeHolder = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment udtPtr = (MemorySegment) MH_NEWEST_UDT.invokeExact(ptr, sizeHolder);
			return RocksDB.toByteArray(udtPtr, sizeHolder.get(ValueLayout.JAVA_LONG, 0));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("MemTableInfo.newestUserDefinedTimestamp failed", t);
		}
	}
}
