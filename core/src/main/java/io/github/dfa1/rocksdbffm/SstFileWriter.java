package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

/// FFM wrapper for `rocksdb_sstfilewriter_t`.
///
/// Writes key-value pairs in sorted order into an SST file on disk, which can
/// then be ingested into a live database via [RocksDB#ingestExternalFile].
///
/// Keys must be written in strictly ascending order (bytewise by default).
///
/// ```
/// Path sstPath = dir.resolve("data.sst");
/// try (var opts = Options.newOptions();
///      var writer = SstFileWriter.newSstFileWriter(opts)) {
///     writer.open(sstPath);
///     writer.put("aaa".getBytes(), "val1".getBytes());
///     writer.put("bbb".getBytes(), "val2".getBytes());
///     writer.finish();
/// }
/// db.ingestExternalFile(List.of(sstPath));
/// ```
public final class SstFileWriter extends NativeObject {

	// rocksdb_envoptions_create(void) -> rocksdb_envoptions_t*
	private static final MethodHandle MH_ENVOPTIONS_CREATE;
	// rocksdb_envoptions_destroy(rocksdb_envoptions_t* opt) -> void
	private static final MethodHandle MH_ENVOPTIONS_DESTROY;
	// rocksdb_sstfilewriter_create(const rocksdb_envoptions_t* env, const rocksdb_options_t* io_options) -> rocksdb_sstfilewriter_t*
	private static final MethodHandle MH_CREATE;
	// rocksdb_sstfilewriter_destroy(rocksdb_sstfilewriter_t* writer) -> void
	private static final MethodHandle MH_DESTROY;
	// rocksdb_sstfilewriter_open(rocksdb_sstfilewriter_t* writer, const char* name, char** errptr) -> void
	private static final MethodHandle MH_OPEN;
	// rocksdb_sstfilewriter_put(rocksdb_sstfilewriter_t* writer, const char* key, size_t keylen, const char* val, size_t vallen, char** errptr) -> void
	private static final MethodHandle MH_PUT;
	// rocksdb_sstfilewriter_delete(rocksdb_sstfilewriter_t* writer, const char* key, size_t keylen, char** errptr) -> void
	private static final MethodHandle MH_DELETE;
	// rocksdb_sstfilewriter_delete_range(rocksdb_sstfilewriter_t* writer, const char* begin_key, size_t begin_keylen, const char* end_key, size_t end_keylen, char** errptr) -> void
	private static final MethodHandle MH_DELETE_RANGE;
	// rocksdb_sstfilewriter_merge(rocksdb_sstfilewriter_t* writer, const char* key, size_t keylen, const char* val, size_t vallen, char** errptr) -> void
	private static final MethodHandle MH_MERGE;
	// rocksdb_sstfilewriter_finish(rocksdb_sstfilewriter_t* writer, char** errptr) -> void
	private static final MethodHandle MH_FINISH;
	// rocksdb_sstfilewriter_file_size(rocksdb_sstfilewriter_t* writer, uint64_t* file_size) -> void
	private static final MethodHandle MH_FILE_SIZE;

	static {
		MH_ENVOPTIONS_CREATE = RocksDB.lookup("rocksdb_envoptions_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_ENVOPTIONS_DESTROY = RocksDB.lookup("rocksdb_envoptions_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		// rocksdb_sstfilewriter_t* rocksdb_sstfilewriter_create(envoptions*, options*)
		MH_CREATE = RocksDB.lookup("rocksdb_sstfilewriter_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_DESTROY = RocksDB.lookup("rocksdb_sstfilewriter_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		// void rocksdb_sstfilewriter_open(writer*, name*, errptr**)
		MH_OPEN = RocksDB.lookup("rocksdb_sstfilewriter_open",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		// void rocksdb_sstfilewriter_put(writer*, key*, keylen, val*, vallen, errptr**)
		MH_PUT = RocksDB.lookup("rocksdb_sstfilewriter_put",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		// void rocksdb_sstfilewriter_delete(writer*, key*, keylen, errptr**)
		MH_DELETE = RocksDB.lookup("rocksdb_sstfilewriter_delete",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		// void rocksdb_sstfilewriter_delete_range(writer*, begin_key*, begin_keylen, end_key*, end_keylen, errptr**)
		MH_DELETE_RANGE = RocksDB.lookup("rocksdb_sstfilewriter_delete_range",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		// void rocksdb_sstfilewriter_merge(writer*, key*, keylen, val*, vallen, errptr**)
		MH_MERGE = RocksDB.lookup("rocksdb_sstfilewriter_merge",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		// void rocksdb_sstfilewriter_finish(writer*, errptr**)
		MH_FINISH = RocksDB.lookup("rocksdb_sstfilewriter_finish",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		// void rocksdb_sstfilewriter_file_size(writer*, uint64_t* file_size)
		MH_FILE_SIZE = RocksDB.lookup("rocksdb_sstfilewriter_file_size",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
	}

	private SstFileWriter(MemorySegment ptr) {
		super(ptr);
	}

	/// Creates an SST file writer using the given DB options (comparator, compression, etc.).
	public static SstFileWriter newSstFileWriter(Options options) {
		try {
			MemorySegment envOpts = (MemorySegment) MH_ENVOPTIONS_CREATE.invokeExact();
			try {
				return new SstFileWriter((MemorySegment) MH_CREATE.invokeExact(envOpts, options.ptr()));
			} finally {
				MH_ENVOPTIONS_DESTROY.invokeExact(envOpts);
			}
		} catch (Throwable t) {
			throw new RocksDBException("sstfilewriter create failed", t);
		}
	}

	/// Opens a new SST file at the given path for writing.
	/// Call [#finish()] to finalize the file before ingesting.
	public void open(Path path) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MH_OPEN.invokeExact(ptr(), pathSeg, err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("sstfilewriter open failed", t);
		}
	}

	/// Appends a put entry. Keys must be added in strictly ascending order.
	public void put(byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment keyNative = Native.toNative(arena, key);
			MemorySegment valNative = Native.toNative(arena, value);
			MH_PUT.invokeExact(ptr(),
					keyNative, (long) key.length,
					valNative, (long) value.length,
					err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("sstfilewriter put failed", t);
		}
	}

	/// Zero-copy [MemorySegment] overload of [#put(byte\[\], byte\[\])].
	public void put(MemorySegment key, MemorySegment value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_PUT.invokeExact(ptr(),
					key, key.byteSize(),
					value, value.byteSize(),
					err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("sstfilewriter put failed", t);
		}
	}

	/// Appends a delete tombstone entry. Keys must be added in strictly ascending order.
	public void delete(byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment keyNative = Native.toNative(arena, key);
			MH_DELETE.invokeExact(ptr(), keyNative, (long) key.length, err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("sstfilewriter delete failed", t);
		}
	}

	/// Zero-copy [MemorySegment] overload of [#delete(byte\[\])].
	public void delete(MemorySegment key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_DELETE.invokeExact(ptr(), key, key.byteSize(), err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("sstfilewriter delete failed", t);
		}
	}

	/// Appends a delete-range tombstone covering `[beginKey, endKey)`.
	/// Keys must be added in strictly ascending order.
	public void deleteRange(byte[] beginKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment beginNative = Native.toNative(arena, beginKey);
			MemorySegment endNative = Native.toNative(arena, endKey);
			MH_DELETE_RANGE.invokeExact(ptr(),
					beginNative, (long) beginKey.length,
					endNative, (long) endKey.length,
					err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("sstfilewriter deleteRange failed", t);
		}
	}

	/// Appends a merge entry. Keys must be added in strictly ascending order.
	public void merge(byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment keyNative = Native.toNative(arena, key);
			MemorySegment valNative = Native.toNative(arena, value);
			MH_MERGE.invokeExact(ptr(),
					keyNative, (long) key.length,
					valNative, (long) value.length,
					err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("sstfilewriter merge failed", t);
		}
	}

	/// Finalizes the SST file. Must be called after all entries have been written.
	/// The file is now ready for ingestion via [RocksDB#ingestExternalFile].
	public void finish() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_FINISH.invokeExact(ptr(), err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("sstfilewriter finish failed", t);
		}
	}

	/// Returns the size of the current (or most recently finished) SST file in bytes.
	public long fileSize() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment sizeSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MH_FILE_SIZE.invokeExact(ptr(), sizeSeg);
			return sizeSeg.get(ValueLayout.JAVA_LONG, 0);
		} catch (Throwable t) {
			throw RocksDBException.wrap("sstfilewriter fileSize failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
