package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;

/**
 * FFM wrapper for rocksdb_writebatch_t.
 * Accumulates put/delete operations and commits them atomically via RocksDB.write().
 * <p>
 * Note: rocksdb_writebatch_put/delete have no errptr — they are infallible at
 * the C level. Only rocksdb_write (on the DB) can fail.
 */
public final class WriteBatch extends NativeObject {

	private static final MethodHandle MH_CREATE;
	private static final MethodHandle MH_DESTROY;
	private static final MethodHandle MH_PUT;
	private static final MethodHandle MH_DELETE;
	private static final MethodHandle MH_MERGE;
	private static final MethodHandle MH_DELETE_RANGE;
	private static final MethodHandle MH_CLEAR;
	private static final MethodHandle MH_COUNT;

	static {
		MH_CREATE = RocksDB.lookup("rocksdb_writebatch_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = RocksDB.lookup("rocksdb_writebatch_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		// void rocksdb_writebatch_put(batch*, key*, klen, val*, vlen)
		MH_PUT = RocksDB.lookup("rocksdb_writebatch_put",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		// void rocksdb_writebatch_delete(batch*, key*, klen)
		MH_DELETE = RocksDB.lookup("rocksdb_writebatch_delete",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		// void rocksdb_writebatch_merge(batch*, key*, klen, val*, vlen)
		MH_MERGE = RocksDB.lookup("rocksdb_writebatch_merge",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		// void rocksdb_writebatch_delete_range(batch*, start*, slen, end*, elen)
		MH_DELETE_RANGE = RocksDB.lookup("rocksdb_writebatch_delete_range",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_CLEAR = RocksDB.lookup("rocksdb_writebatch_clear",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_COUNT = RocksDB.lookup("rocksdb_writebatch_count",
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

	/**
	 * Queues a merge operand for {@code key}. Slow path: copies key/value.
	 */
	public void merge(byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MH_MERGE.invokeExact(ptr(),
					Native.toNative(arena, key), (long) key.length,
					Native.toNative(arena, value), (long) value.length);
		} catch (Throwable t) {
			throw new RocksDBException("writebatch merge failed", t);
		}
	}

	/**
	 * Queues a merge operand for {@code key}. Zero-copy for direct buffers.
	 */
	public void merge(ByteBuffer key, ByteBuffer value) {
		try {
			MH_MERGE.invokeExact(ptr(),
					MemorySegment.ofBuffer(key), (long) key.remaining(),
					MemorySegment.ofBuffer(value), (long) value.remaining());
		} catch (Throwable t) {
			throw new RocksDBException("writebatch merge failed", t);
		}
	}

	/**
	 * Queues a merge operand for {@code key}. Zero-copy.
	 */
	public void merge(MemorySegment key, MemorySegment value) {
		try {
			MH_MERGE.invokeExact(ptr(),
					key, key.byteSize(),
					value, value.byteSize());
		} catch (Throwable t) {
			throw new RocksDBException("writebatch merge failed", t);
		}
	}

	/**
	 * Queues a range tombstone for [{@code startKey}, {@code endKey}). Slow path: copies keys.
	 */
	public void deleteRange(byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MH_DELETE_RANGE.invokeExact(ptr(),
					Native.toNative(arena, startKey), (long) startKey.length,
					Native.toNative(arena, endKey), (long) endKey.length);
		} catch (Throwable t) {
			throw new RocksDBException("writebatch deleteRange failed", t);
		}
	}

	/**
	 * Queues a range tombstone for [{@code startKey}, {@code endKey}). Zero-copy for direct buffers.
	 */
	public void deleteRange(ByteBuffer startKey, ByteBuffer endKey) {
		try {
			MH_DELETE_RANGE.invokeExact(ptr(),
					MemorySegment.ofBuffer(startKey), (long) startKey.remaining(),
					MemorySegment.ofBuffer(endKey), (long) endKey.remaining());
		} catch (Throwable t) {
			throw new RocksDBException("writebatch deleteRange failed", t);
		}
	}

	/**
	 * Queues a range tombstone for [{@code startKey}, {@code endKey}). Zero-copy.
	 */
	public void deleteRange(MemorySegment startKey, MemorySegment endKey) {
		try {
			MH_DELETE_RANGE.invokeExact(ptr(),
					startKey, startKey.byteSize(),
					endKey, endKey.byteSize());
		} catch (Throwable t) {
			throw new RocksDBException("writebatch deleteRange failed", t);
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
