package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;

/// FFM wrapper for `rocksdb_iterator_t`.
///
/// Obtain via [RocksDB#newIterator()] or [RocksDB#newIterator(ReadOptions)].
/// Always close after use.
///
/// ```
/// try (RocksIterator it = db.newIterator()) {
///     for (it.seekToFirst(); it.isValid(); it.next()) {
///         byte[] key   = it.key();
///         byte[] value = it.value();
///     }
///     it.checkError();
/// }
/// ```
public final class RocksIterator extends NativeObject {

	/// `rocksdb_iterator_t* rocksdb_create_iterator(rocksdb_t* db, const rocksdb_readoptions_t* options);`
	private static final MethodHandle MH_CREATE;
	/// `void rocksdb_iter_destroy(rocksdb_iterator_t*);`
	private static final MethodHandle MH_DESTROY;
	/// `unsigned char rocksdb_iter_valid(const rocksdb_iterator_t*);`
	private static final MethodHandle MH_VALID;
	/// `void rocksdb_iter_seek_to_first(rocksdb_iterator_t*);`
	private static final MethodHandle MH_SEEK_TO_FIRST;
	/// `void rocksdb_iter_seek_to_last(rocksdb_iterator_t*);`
	private static final MethodHandle MH_SEEK_TO_LAST;
	/// `void rocksdb_iter_seek(rocksdb_iterator_t*, const char* k, size_t klen);`
	private static final MethodHandle MH_SEEK;
	/// `void rocksdb_iter_seek_for_prev(rocksdb_iterator_t*, const char* k, size_t klen);`
	private static final MethodHandle MH_SEEK_FOR_PREV;
	/// `void rocksdb_iter_next(rocksdb_iterator_t*);`
	private static final MethodHandle MH_NEXT;
	/// `void rocksdb_iter_prev(rocksdb_iterator_t*);`
	private static final MethodHandle MH_PREV;
	/// `const char* rocksdb_iter_key(const rocksdb_iterator_t*, size_t* klen);`
	private static final MethodHandle MH_KEY;
	/// `const char* rocksdb_iter_value(const rocksdb_iterator_t*, size_t* vlen);`
	private static final MethodHandle MH_VALUE;
	/// `void rocksdb_iter_get_error(const rocksdb_iterator_t*, char** errptr);`
	private static final MethodHandle MH_GET_ERROR;
	/// `void rocksdb_iter_refresh(const rocksdb_iterator_t* iter, char** errptr);`
	private static final MethodHandle MH_REFRESH;

	static {
		MH_CREATE = RocksDB.lookup("rocksdb_create_iterator",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_DESTROY = RocksDB.lookup("rocksdb_iter_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_VALID = RocksDB.lookup("rocksdb_iter_valid",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_SEEK_TO_FIRST = RocksDB.lookup("rocksdb_iter_seek_to_first",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SEEK_TO_LAST = RocksDB.lookup("rocksdb_iter_seek_to_last",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SEEK = RocksDB.lookup("rocksdb_iter_seek",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_SEEK_FOR_PREV = RocksDB.lookup("rocksdb_iter_seek_for_prev",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_NEXT = RocksDB.lookup("rocksdb_iter_next",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_PREV = RocksDB.lookup("rocksdb_iter_prev",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_KEY = RocksDB.lookup("rocksdb_iter_key",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_VALUE = RocksDB.lookup("rocksdb_iter_value",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_GET_ERROR = RocksDB.lookup("rocksdb_iter_get_error",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_REFRESH = RocksDB.lookup("rocksdb_iter_refresh",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
	}

	// Reused across all key/value calls to avoid per-call Arena allocation.
	private final Arena lenArena;
	private final MemorySegment lenSegment;

	/// Package-private: created via [#create].
	private RocksIterator(MemorySegment ptr) {
		super(ptr);
		this.lenArena = Arena.ofConfined();
		this.lenSegment = lenArena.allocate(ValueLayout.JAVA_LONG);
	}

	/// Package-private factory called by RocksDB.
	static RocksIterator create(MemorySegment dbPtr, MemorySegment readOptions) {
		try {
			MemorySegment iterPtr = (MemorySegment) MH_CREATE.invokeExact(dbPtr, readOptions);
			return new RocksIterator(iterPtr);
		} catch (Throwable t) {
			throw RocksDBException.wrap("iterator create failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Positioning
	// -----------------------------------------------------------------------

	/// Positions the iterator at the first key in the database.
	public void seekToFirst() {
		try {
			MH_SEEK_TO_FIRST.invokeExact(ptr());
		} catch (Throwable t) {
			throw RocksDBException.wrap("seekToFirst failed", t);
		}
	}

	/// Positions the iterator at the last key in the database.
	public void seekToLast() {
		try {
			MH_SEEK_TO_LAST.invokeExact(ptr());
		} catch (Throwable t) {
			throw RocksDBException.wrap("seekToLast failed", t);
		}
	}

	/// Positions the iterator at the first key >= `target`. Slow path: copies key.
	public void seek(byte[] target) {
		try (Arena arena = Arena.ofConfined()) {
			MH_SEEK.invokeExact(ptr(), Native.toNative(arena, target), (long) target.length);
		} catch (Throwable t) {
			throw RocksDBException.wrap("seek failed", t);
		}
	}

	/// Positions the iterator at the first key >= `target`. Zero-copy for direct buffers.
	public void seek(ByteBuffer target) {
		try {
			MH_SEEK.invokeExact(ptr(), MemorySegment.ofBuffer(target), (long) target.remaining());
		} catch (Throwable t) {
			throw RocksDBException.wrap("seek failed", t);
		}
	}

	/// Positions the iterator at the first key >= `target`. Zero-copy.
	public void seek(MemorySegment target) {
		try {
			MH_SEEK.invokeExact(ptr(), target, target.byteSize());
		} catch (Throwable t) {
			throw RocksDBException.wrap("seek failed", t);
		}
	}

	/// Positions the iterator at the last key <= `target`. Slow path: copies key.
	public void seekForPrev(byte[] target) {
		try (Arena arena = Arena.ofConfined()) {
			MH_SEEK_FOR_PREV.invokeExact(ptr(), Native.toNative(arena, target), (long) target.length);
		} catch (Throwable t) {
			throw RocksDBException.wrap("seekForPrev failed", t);
		}
	}

	/// Positions the iterator at the last key <= `target`. Zero-copy for direct buffers.
	public void seekForPrev(ByteBuffer target) {
		try {
			MH_SEEK_FOR_PREV.invokeExact(ptr(), MemorySegment.ofBuffer(target), (long) target.remaining());
		} catch (Throwable t) {
			throw RocksDBException.wrap("seekForPrev failed", t);
		}
	}

	/// Positions the iterator at the last key <= `target`. Zero-copy.
	public void seekForPrev(MemorySegment target) {
		try {
			MH_SEEK_FOR_PREV.invokeExact(ptr(), target, target.byteSize());
		} catch (Throwable t) {
			throw RocksDBException.wrap("seekForPrev failed", t);
		}
	}

	/// Moves to the next key. Only call when [#isValid()] is true.
	public void next() {
		try {
			MH_NEXT.invokeExact(ptr());
		} catch (Throwable t) {
			throw RocksDBException.wrap("next failed", t);
		}
	}

	/// Moves to the previous key. Only call when [#isValid()] is true.
	public void prev() {
		try {
			MH_PREV.invokeExact(ptr());
		} catch (Throwable t) {
			throw RocksDBException.wrap("prev failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// State
	// -----------------------------------------------------------------------

	/// Returns true if the iterator is positioned at a valid key.
	public boolean isValid() {
		try {
			return ((byte) MH_VALID.invokeExact(ptr())) != 0;
		} catch (Throwable t) {
			throw RocksDBException.wrap("isValid failed", t);
		}
	}

	/// Checks for any I/O error encountered during iteration.
	/// Always call after an iteration loop to detect background errors.
	/// Throws [RocksDBException] if an error occurred.
	public void checkError() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_GET_ERROR.invokeExact(ptr(), err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getError failed", t);
		}
	}

	/// Refreshes the iterator to reflect the latest DB state after mutations.
	/// Repositions to the same key if it still exists.
	public void refresh() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_REFRESH.invokeExact(ptr(), err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("refresh failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Key/Value access — MemorySegment (zero-copy, valid until next navigation call)
	// -----------------------------------------------------------------------

	/// Returns a zero-copy view of the current key.
	/// The returned segment is only valid until the next positioning call.
	/// Only call when [#isValid()] is true.
	public MemorySegment keySegment() {
		try {
			MemorySegment data = (MemorySegment) MH_KEY.invokeExact(ptr(), lenSegment);
			return data.reinterpret(lenSegment.get(ValueLayout.JAVA_LONG, 0));
		} catch (Throwable t) {
			throw RocksDBException.wrap("key failed", t);
		}
	}

	/// Returns a zero-copy view of the current value.
	/// The returned segment is only valid until the next positioning call.
	/// Only call when [#isValid()] is true.
	public MemorySegment valueSegment() {
		try {
			MemorySegment data = (MemorySegment) MH_VALUE.invokeExact(ptr(), lenSegment);
			return data.reinterpret(lenSegment.get(ValueLayout.JAVA_LONG, 0));
		} catch (Throwable t) {
			throw RocksDBException.wrap("value failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Key/Value access — ByteBuffer (single copy into caller's buffer)
	// -----------------------------------------------------------------------

	/// Copies the current key into `dst`. Returns the actual key length.
	/// Only call when [#isValid()] is true.
	public int key(ByteBuffer dst) {
		try {
			MemorySegment data = (MemorySegment) MH_KEY.invokeExact(ptr(), lenSegment);
			long len = lenSegment.get(ValueLayout.JAVA_LONG, 0);
			int toCopy = (int) Math.min(len, dst.remaining());
			MemorySegment.ofBuffer(dst).copyFrom(data.reinterpret(toCopy));
			dst.position(dst.position() + toCopy);
			return (int) len;
		} catch (Throwable t) {
			throw RocksDBException.wrap("key failed", t);
		}
	}

	/// Copies the current value into `dst`. Returns the actual value length.
	/// Only call when [#isValid()] is true.
	public int value(ByteBuffer dst) {
		try {
			MemorySegment data = (MemorySegment) MH_VALUE.invokeExact(ptr(), lenSegment);
			long len = lenSegment.get(ValueLayout.JAVA_LONG, 0);
			int toCopy = (int) Math.min(len, dst.remaining());
			MemorySegment.ofBuffer(dst).copyFrom(data.reinterpret(toCopy));
			dst.position(dst.position() + toCopy);
			return (int) len;
		} catch (Throwable t) {
			throw RocksDBException.wrap("value failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Key/Value access — byte[] (convenience, always copies)
	// -----------------------------------------------------------------------

	/// Returns a copy of the current key as a byte array.
	/// Slower than the MemorySegment or ByteBuffer variants due to heap allocation.
	/// Only call when [#isValid()] is true.
	public byte[] key() {
		try {
			MemorySegment data = (MemorySegment) MH_KEY.invokeExact(ptr(), lenSegment);
			return data.reinterpret(lenSegment.get(ValueLayout.JAVA_LONG, 0)).toArray(ValueLayout.JAVA_BYTE);
		} catch (Throwable t) {
			throw RocksDBException.wrap("key failed", t);
		}
	}

	/// Returns a copy of the current value as a byte array.
	/// Slower than the MemorySegment or ByteBuffer variants due to heap allocation.
	/// Only call when [#isValid()] is true.
	public byte[] value() {
		try {
			MemorySegment data = (MemorySegment) MH_VALUE.invokeExact(ptr(), lenSegment);
			return data.reinterpret(lenSegment.get(ValueLayout.JAVA_LONG, 0)).toArray(ValueLayout.JAVA_BYTE);
		} catch (Throwable t) {
			throw RocksDBException.wrap("value failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// AutoCloseable
	// -----------------------------------------------------------------------

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
		lenArena.close();
	}
}
