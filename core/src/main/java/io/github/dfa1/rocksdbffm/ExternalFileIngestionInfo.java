package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

/// Read-only view of an external SST file ingestion, passed to
/// [EventNotifier#onExternalFileIngested(ExternalFileIngestionInfo)].
///
/// Wraps a `const rocksdb_externalfileingestioninfo_t*` owned by RocksDB for the duration of the
/// callback only: every accessor reads through that pointer on demand, so an instance must never
/// be retained or used after the callback method returns.
public final class ExternalFileIngestionInfo {

	/// `const char* rocksdb_externalfileingestioninfo_cf_name(const rocksdb_externalfileingestioninfo_t*, size_t* size);`
	private static final MethodHandle MH_CF_NAME = NativeLibrary.lookup(
			"rocksdb_externalfileingestioninfo_cf_name",
			FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

	/// `const char* rocksdb_externalfileingestioninfo_external_file_path(const rocksdb_externalfileingestioninfo_t*, size_t* size);`
	private static final MethodHandle MH_EXTERNAL_FILE_PATH = NativeLibrary.lookup(
			"rocksdb_externalfileingestioninfo_external_file_path",
			FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

	/// `const char* rocksdb_externalfileingestioninfo_internal_file_path(const rocksdb_externalfileingestioninfo_t*, size_t* size);`
	private static final MethodHandle MH_INTERNAL_FILE_PATH = NativeLibrary.lookup(
			"rocksdb_externalfileingestioninfo_internal_file_path",
			FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

	/// `uint64_t rocksdb_externalfileingestioninfo_global_seqno(const rocksdb_externalfileingestioninfo_t*);`
	private static final MethodHandle MH_GLOBAL_SEQNO = NativeLibrary.lookup(
			"rocksdb_externalfileingestioninfo_global_seqno",
			FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

	private final MemorySegment ptr;

	ExternalFileIngestionInfo(MemorySegment ptr) {
		this.ptr = ptr;
	}

	/// Returns the name of the column family the file was ingested into.
	///
	/// @return the column family name
	public String columnFamilyName() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment sizeHolder = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment namePtr = (MemorySegment) MH_CF_NAME.invokeExact(ptr, sizeHolder);
			return RocksDB.toJavaString(namePtr, sizeHolder.get(ValueLayout.JAVA_LONG, 0));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("ExternalFileIngestionInfo.columnFamilyName failed", t);
		}
	}

	/// Returns the path of the file that was ingested, as supplied by the caller.
	///
	/// @return the external file's original path
	public Path externalFilePath() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment sizeHolder = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment pathPtr = (MemorySegment) MH_EXTERNAL_FILE_PATH.invokeExact(ptr, sizeHolder);
			return Path.of(RocksDB.toJavaString(pathPtr, sizeHolder.get(ValueLayout.JAVA_LONG, 0)));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("ExternalFileIngestionInfo.externalFilePath failed", t);
		}
	}

	/// Returns the path the file was moved or copied to inside the database directory.
	///
	/// @return the ingested file's path inside the database
	public Path internalFilePath() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment sizeHolder = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment pathPtr = (MemorySegment) MH_INTERNAL_FILE_PATH.invokeExact(ptr, sizeHolder);
			return Path.of(RocksDB.toJavaString(pathPtr, sizeHolder.get(ValueLayout.JAVA_LONG, 0)));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("ExternalFileIngestionInfo.internalFilePath failed", t);
		}
	}

	/// Returns the sequence number assigned to every key in the ingested file.
	///
	/// @return the global sequence number
	public SequenceNumber globalSequenceNumber() {
		try {
			return SequenceNumber.of((long) MH_GLOBAL_SEQNO.invokeExact(ptr));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("ExternalFileIngestionInfo.globalSequenceNumber failed", t);
		}
	}
}
