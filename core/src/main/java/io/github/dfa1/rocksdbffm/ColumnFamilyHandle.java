package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;

/// FFM wrapper for `rocksdb_column_family_handle_t`.
///
/// Obtained via [RocksDB#openWithColumnFamilies] or [ReadWriteDB#createColumnFamily].
/// Must be closed after use to release the native handle.
///
/// ```
/// try (var db = RocksDB.open(path);
///      var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"))) {
///     db.put(cf, "key".getBytes(), "value".getBytes());
/// }
/// ```
public final class ColumnFamilyHandle extends NativeObject {

	/// `void rocksdb_column_family_handle_destroy(rocksdb_column_family_handle_t*);`
	private static final MethodHandle MH_DESTROY;
	/// `uint32_t rocksdb_column_family_handle_get_id(rocksdb_column_family_handle_t* handle);`
	private static final MethodHandle MH_GET_ID;
	/// `char* rocksdb_column_family_handle_get_name(rocksdb_column_family_handle_t* handle, size_t* name_len);`
	private static final MethodHandle MH_GET_NAME;

	static {
		MH_DESTROY = NativeLibrary.lookup("rocksdb_column_family_handle_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_GET_ID = NativeLibrary.lookup("rocksdb_column_family_handle_get_id",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_GET_NAME = NativeLibrary.lookup("rocksdb_column_family_handle_get_name",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));
	}

	private ColumnFamilyHandle(MemorySegment ptr) {
		super(ptr);
	}

	static ColumnFamilyHandle wrap(MemorySegment ptr) {
		return new ColumnFamilyHandle(ptr);
	}

	/// Returns the numeric ID of this column family.
	public int getId() {
		try {
			return (int) MH_GET_ID.invokeExact(ptr());
		} catch (Throwable t) {
			throw RocksDBException.wrap("getId failed", t);
		}
	}

	/// Returns the name of this column family.
	public String getName() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment lenSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment namePtr = (MemorySegment) MH_GET_NAME.invokeExact(ptr(), lenSeg);
			long nameLen = lenSeg.get(ValueLayout.JAVA_LONG, 0);
			byte[] bytes = namePtr.reinterpret(nameLen).toArray(ValueLayout.JAVA_BYTE);
			return new String(bytes, StandardCharsets.UTF_8);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getName failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
