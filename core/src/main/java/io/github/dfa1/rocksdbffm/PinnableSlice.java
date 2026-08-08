package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// A borrowed view of a value still held in RocksDB's block cache.
///
/// `rocksdb_get_pinned` returns a pointer directly into the cached block instead of
/// copying the value out, which is what makes the pinned read path zero-copy. The
/// cost is that the cache entry cannot be evicted while this object is open, so a
/// slice held for a long time is back-pressure on the block cache — it presents as
/// native memory growth, not as Java heap growth. Close it promptly; the intended
/// shape is a try-with-resources whose body does not outlive the statement.
///
/// The segment returned by [#value()] is scoped to this slice: once [#close()] runs,
/// touching it throws [IllegalStateException] rather than reading freed memory.
public final class PinnableSlice extends NativeObject {

	/// `const char* rocksdb_pinnableslice_value(const rocksdb_pinnableslice_t* t, size_t* vlen);`
	private static final MethodHandle MH_PINNABLESLICE_VALUE;
	/// `void rocksdb_pinnableslice_destroy(rocksdb_pinnableslice_t* v);`
	private static final MethodHandle MH_PINNABLESLICE_DESTROY;

	static {
		MH_PINNABLESLICE_VALUE = NativeLibrary.lookup("rocksdb_pinnableslice_value",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PINNABLESLICE_DESTROY = NativeLibrary.lookup("rocksdb_pinnableslice_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
	}

	/// Scope for the segment handed out by [#value()], created on first use so callers
	/// who never look at the bytes do not pay for it.
	///
	/// Deliberately **shared**, not confined. A confined arena can only be closed by
	/// the thread that created it, and [#close()] runs inside
	/// [NativeObject#close()], which swallows what `tryClose` throws — so a confined
	/// scope closed from another thread would fail silently and leave this segment
	/// readable over memory the pin no longer holds. A shared arena closes from any
	/// thread, which keeps "use after close throws" true regardless of who closes.
	private Arena valueArena;
	private MemorySegment value;

	private PinnableSlice(MemorySegment owningPointer) {
		super(owningPointer);
	}

	/// Wraps a non-NULL `rocksdb_pinnableslice_t*`.
	static PinnableSlice of(MemorySegment owningPointer) {
		return new PinnableSlice(owningPointer);
	}

	/// Returns the pinned value as a segment whose `byteSize()` is the value length.
	///
	/// The segment borrows RocksDB's memory — it is not a copy, and it is valid only
	/// until this slice is closed. Repeated calls return the same segment.
	///
	/// @return the pinned value, sized to the value's true length
	/// @throws IllegalStateException if this slice has already been closed
	public MemorySegment value() {
		if (value == null) {
			MemorySegment ptr = ptr();
			Arena arena = Arena.ofShared();
			try {
				value = rawValue(ptr, arena, arena);
			} catch (RuntimeException | Error e) {
				arena.close();
				throw e;
			}
			valueArena = arena;
		}
		return value;
	}

	/// Copies the pinned value into a new array.
	///
	/// Convenience for callers who want the bytes on the Java heap; this is the
	/// copying path and defeats the point of pinning, so prefer [#value()].
	///
	/// @return a fresh array holding a copy of the pinned value
	/// @throws IllegalStateException if this slice has already been closed
	public byte[] toArray() {
		return value().toArray(ValueLayout.JAVA_BYTE);
	}

	/// Returns the length of the pinned value in bytes.
	///
	/// @return the value length
	/// @throws IllegalStateException if this slice has already been closed
	public long length() {
		return value().byteSize();
	}

	// -----------------------------------------------------------------------
	// Raw-pointer statics
	//
	// The copying get helpers own a rocksdb_pinnableslice_t* for the length of one
	// call, copy the bytes out, and destroy it. Wrapping it would allocate a
	// PinnableSlice and a PinnedResult per read on the hottest path in the library,
	// which measured as a 1.5-3.5% throughput regression across all three read tiers.
	// These statics let those helpers share this class's method handles -- so the
	// symbols are still mapped exactly once -- without allocating anything.
	// -----------------------------------------------------------------------

	/// Reads the value out of a raw `rocksdb_pinnableslice_t*` without wrapping it.
	///
	/// The returned segment is unscoped and borrows RocksDB's memory: it is valid only
	/// until [#destroy(MemorySegment)] is called on the same pointer.
	static MemorySegment valueOf(MemorySegment pin, Arena scratch) {
		return rawValue(pin, scratch, null);
	}

	/// Reads the value out of a raw `rocksdb_pinnableslice_t*` using a length holder the
	/// caller already allocated, so a loop of reads allocates nothing at all.
	///
	/// Experiment hook for the reuse question on #47: it measures the ceiling of what a
	/// caller-owned, reused slice could save on the Java side. The returned segment is
	/// unscoped — valid only until [#destroy(MemorySegment)] — which is precisely the
	/// safety that [#value()] buys with its arena.
	static MemorySegment valueInto(MemorySegment pin, MemorySegment lenHolder) {
		try {
			MemorySegment data = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, lenHolder);
			return data.reinterpret(lenHolder.get(ValueLayout.JAVA_LONG, 0));
		} catch (Throwable t) {
			throw RocksDBException.wrap("pinnableslice value failed", t);
		}
	}

	/// Destroys a raw `rocksdb_pinnableslice_t*`. Safe on [MemorySegment#NULL] — the C
	/// API's destroy is a plain `delete`, which tolerates a null pointer.
	static void destroy(MemorySegment pin) {
		try {
			MH_PINNABLESLICE_DESTROY.invokeExact(pin);
		} catch (Throwable t) {
			throw RocksDBException.wrap("pinnableslice destroy failed", t);
		}
	}

	/// Reads the value pointer and length, sizing the result to `scope` (or leaving it
	/// unscoped when `scope` is `null`, in which case the segment must not escape while
	/// the underlying slice can still be destroyed).
	private static MemorySegment rawValue(MemorySegment pin, Arena scratch, Arena scope) {
		try {
			MemorySegment lenHolder = scratch.allocate(ValueLayout.JAVA_LONG);
			MemorySegment data = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, lenHolder);
			long len = lenHolder.get(ValueLayout.JAVA_LONG, 0);
			return scope == null ? data.reinterpret(len) : data.reinterpret(len, scope, null);
		} catch (Throwable t) {
			throw RocksDBException.wrap("pinnableslice value failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		value = null;
		try {
			// Close the arena first so any segment handed out by value() is invalidated
			// before the memory behind it is released — a use-after-close then throws
			// rather than reading freed memory.
			if (valueArena != null) {
				valueArena.close();
				valueArena = null;
			}
		} finally {
			// But release the pin even if that failed. valueArena is confined, so
			// closing this slice from a different thread throws WrongThreadException —
			// and NativeObject.close() swallows it after having already nulled the
			// pointer, so a skipped destroy would strand the block-cache entry forever
			// with nothing able to retry. The finally makes the native release
			// unconditional; the worst a failed arena close now costs is 16 bytes of
			// scratch and a segment that outlives its memory.
			MH_PINNABLESLICE_DESTROY.invokeExact(ptr);
		}
	}
}
