package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;

/// FFM wrapper for `rocksdb_writebatch_t`.
/// Accumulates put/delete operations and commits them atomically via RocksDB.write().
///
/// Note: `rocksdb_writebatch\put/delete` have no `errptr` — they are infallible at
/// the C level. Only `rocksdb_write` (on the DB) can fail.
public final class WriteBatch extends NativeObject {

	/// `rocksdb_writebatch_t* rocksdb_writebatch_create(void);`
	private static final MethodHandle MH_CREATE;
	/// `void rocksdb_writebatch_destroy(rocksdb_writebatch_t*);`
	private static final MethodHandle MH_DESTROY;
	/// `void rocksdb_writebatch_put(rocksdb_writebatch_t*, const char* key, size_t klen, const char* val, size_t vlen);`
	private static final MethodHandle MH_PUT;
	/// `void rocksdb_writebatch_delete(rocksdb_writebatch_t*, const char* key, size_t klen);`
	private static final MethodHandle MH_DELETE;
	/// `void rocksdb_writebatch_delete_range(rocksdb_writebatch_t* b, const char* start_key, size_t start_key_len, const char* end_key, size_t end_key_len);`
	private static final MethodHandle MH_DELETE_RANGE;
	/// `void rocksdb_writebatch_put_cf(rocksdb_writebatch_t*, rocksdb_column_family_handle_t* column_family, const char* key, size_t klen, const char* val, size_t vlen);`
	private static final MethodHandle MH_PUT_CF;
	/// `void rocksdb_writebatch_delete_cf(rocksdb_writebatch_t*, rocksdb_column_family_handle_t* column_family, const char* key, size_t klen);`
	private static final MethodHandle MH_DELETE_CF;
	/// `void rocksdb_writebatch_delete_range_cf(rocksdb_writebatch_t* b, rocksdb_column_family_handle_t* column_family, const char* start_key, size_t start_key_len, const char* end_key, size_t end_key_len);`
	private static final MethodHandle MH_DELETE_RANGE_CF;
	/// `void rocksdb_writebatch_clear(rocksdb_writebatch_t*);`
	private static final MethodHandle MH_CLEAR;
	/// `int rocksdb_writebatch_count(rocksdb_writebatch_t*);`
	private static final MethodHandle MH_COUNT;

	static {
		MH_CREATE = NativeLibrary.lookup("rocksdb_writebatch_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = NativeLibrary.lookup("rocksdb_writebatch_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_PUT = NativeLibrary.lookup("rocksdb_writebatch_put",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_DELETE = NativeLibrary.lookup("rocksdb_writebatch_delete",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_DELETE_RANGE = NativeLibrary.lookup("rocksdb_writebatch_delete_range",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_PUT_CF = NativeLibrary.lookup("rocksdb_writebatch_put_cf",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_DELETE_CF = NativeLibrary.lookup("rocksdb_writebatch_delete_cf",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_DELETE_RANGE_CF = NativeLibrary.lookup("rocksdb_writebatch_delete_range_cf",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_CLEAR = NativeLibrary.lookup("rocksdb_writebatch_clear",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_COUNT = NativeLibrary.lookup("rocksdb_writebatch_count",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
	}

	private WriteBatch(MemorySegment ptr) {
		super(ptr);
	}

	public static WriteBatch create() {
		try {
			return new WriteBatch((MemorySegment) MH_CREATE.invokeExact());
		} catch (Throwable t) {
			throw new RocksDBException("writebatch create failed", t);
		}
	}

	/// Wraps an existing `rocksdb_writebatch_t*` returned by the C API (e.g. from WAL iteration).
	/// The caller is responsible for ensuring the pointer is valid and not already owned.
	static WriteBatch wrap(MemorySegment ptr) {
		return new WriteBatch(ptr);
	}

	// TODO: experimental zig-like interface => but in the end it could be easier just to allocate the arena as part of the batch
	public void put(Arena arena, byte[] key, byte[] value) {
		try {
			MemorySegment k = Native.toNative(arena, key);
			MemorySegment v = Native.toNative(arena, value);
			MH_PUT.invokeExact(ptr(), k, (long) key.length, v, (long) value.length);
		} catch (Throwable t) {
			throw new RocksDBException("writebatch put failed", t);
		}
	}

	public void put(byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment k = Native.toNative(arena, key);
			MemorySegment v = Native.toNative(arena, value);
			MH_PUT.invokeExact(ptr(), k, (long) key.length, v, (long) value.length);
		} catch (Throwable t) {
			throw new RocksDBException("writebatch put failed", t);
		}
	}

	public void delete(byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment k = Native.toNative(arena, key);
			MH_DELETE.invokeExact(ptr(), k, (long) key.length);
		} catch (Throwable t) {
			throw new RocksDBException("writebatch delete failed", t);
		}
	}

	/// Queues a range tombstone for [`startKey`, `endKey`). Slow path: copies keys.
	public void deleteRange(byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MH_DELETE_RANGE.invokeExact(ptr(),
					Native.toNative(arena, startKey), (long) startKey.length,
					Native.toNative(arena, endKey), (long) endKey.length);
		} catch (Throwable t) {
			throw new RocksDBException("writebatch deleteRange failed", t);
		}
	}

	/// Queues a range tombstone for [`startKey`, `endKey`). Zero-copy for direct buffers.
	public void deleteRange(ByteBuffer startKey, ByteBuffer endKey) {
		try {
			MH_DELETE_RANGE.invokeExact(ptr(),
					MemorySegment.ofBuffer(startKey), (long) startKey.remaining(),
					MemorySegment.ofBuffer(endKey), (long) endKey.remaining());
		} catch (Throwable t) {
			throw new RocksDBException("writebatch deleteRange failed", t);
		}
	}

	/// Queues a range tombstone for [`startKey`, `endKey`). Zero-copy.
	public void deleteRange(MemorySegment startKey, MemorySegment endKey) {
		try {
			MH_DELETE_RANGE.invokeExact(ptr(),
					startKey, startKey.byteSize(),
					endKey, endKey.byteSize());
		} catch (Throwable t) {
			throw new RocksDBException("writebatch deleteRange failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Column family overloads
	// -----------------------------------------------------------------------

	/// Queues a put into `cf`. Slow path: copies key/value into native memory.
	public void put(ColumnFamilyHandle cf, byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MH_PUT_CF.invokeExact(ptr(), cf.ptr(),
					Native.toNative(arena, key), (long) key.length,
					Native.toNative(arena, value), (long) value.length);
		} catch (Throwable t) {
			throw new RocksDBException("writebatch put_cf failed", t);
		}
	}

	/// Queues a put into `cf`. Zero-copy for direct [ByteBuffer]s.
	public void put(ColumnFamilyHandle cf, ByteBuffer key, ByteBuffer value) {
		try {
			MH_PUT_CF.invokeExact(ptr(), cf.ptr(),
					MemorySegment.ofBuffer(key), (long) key.remaining(),
					MemorySegment.ofBuffer(value), (long) value.remaining());
		} catch (Throwable t) {
			throw new RocksDBException("writebatch put_cf failed", t);
		}
	}

	/// Queues a put into `cf`. Zero-copy for [MemorySegment]s.
	public void put(ColumnFamilyHandle cf, MemorySegment key, MemorySegment value) {
		try {
			MH_PUT_CF.invokeExact(ptr(), cf.ptr(),
					key, key.byteSize(), value, value.byteSize());
		} catch (Throwable t) {
			throw new RocksDBException("writebatch put_cf failed", t);
		}
	}

	/// Queues a delete from `cf`. Slow path: copies the key into native memory.
	public void delete(ColumnFamilyHandle cf, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MH_DELETE_CF.invokeExact(ptr(), cf.ptr(),
					Native.toNative(arena, key), (long) key.length);
		} catch (Throwable t) {
			throw new RocksDBException("writebatch delete_cf failed", t);
		}
	}

	/// Queues a delete from `cf`. Zero-copy for direct [ByteBuffer]s.
	public void delete(ColumnFamilyHandle cf, ByteBuffer key) {
		try {
			MH_DELETE_CF.invokeExact(ptr(), cf.ptr(),
					MemorySegment.ofBuffer(key), (long) key.remaining());
		} catch (Throwable t) {
			throw new RocksDBException("writebatch delete_cf failed", t);
		}
	}

	/// Queues a delete from `cf`. Zero-copy for [MemorySegment]s.
	public void delete(ColumnFamilyHandle cf, MemorySegment key) {
		try {
			MH_DELETE_CF.invokeExact(ptr(), cf.ptr(), key, key.byteSize());
		} catch (Throwable t) {
			throw new RocksDBException("writebatch delete_cf failed", t);
		}
	}

	/// Queues a range tombstone [`startKey`, `endKey`) in `cf`. Slow path.
	public void deleteRange(ColumnFamilyHandle cf, byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MH_DELETE_RANGE_CF.invokeExact(ptr(), cf.ptr(),
					Native.toNative(arena, startKey), (long) startKey.length,
					Native.toNative(arena, endKey), (long) endKey.length);
		} catch (Throwable t) {
			throw new RocksDBException("writebatch deleteRange_cf failed", t);
		}
	}

	/// Queues a range tombstone [`startKey`, `endKey`) in `cf`. Zero-copy for direct buffers.
	public void deleteRange(ColumnFamilyHandle cf, ByteBuffer startKey, ByteBuffer endKey) {
		try {
			MH_DELETE_RANGE_CF.invokeExact(ptr(), cf.ptr(),
					MemorySegment.ofBuffer(startKey), (long) startKey.remaining(),
					MemorySegment.ofBuffer(endKey), (long) endKey.remaining());
		} catch (Throwable t) {
			throw new RocksDBException("writebatch deleteRange_cf failed", t);
		}
	}

	/// Queues a range tombstone [`startKey`, `endKey`) in `cf`. Zero-copy.
	public void deleteRange(ColumnFamilyHandle cf, MemorySegment startKey, MemorySegment endKey) {
		try {
			MH_DELETE_RANGE_CF.invokeExact(ptr(), cf.ptr(),
					startKey, startKey.byteSize(), endKey, endKey.byteSize());
		} catch (Throwable t) {
			throw new RocksDBException("writebatch deleteRange_cf failed", t);
		}
	}

	public void clear() {
		try {
			MH_CLEAR.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("writebatch clear failed", t);
		}
	}

	public int count() {
		try {
			return (int) MH_COUNT.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("writebatch count failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}

}
