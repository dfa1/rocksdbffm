package io.github.dfa1.rocksdbffm;

import io.github.dfa1.rocksdbffm.pool.BlockingPool;
import io.github.dfa1.rocksdbffm.pool.CachedBlockingPool;
import io.github.dfa1.rocksdbffm.pool.Pool;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * Centralized utility for RocksDB native operations.
 * Handles symbol lookup, error checking, and common memory patterns.
 * NB: this is package private
 */
final class Native {

	private static final MethodHandle MH_FREE = RocksDB.lookup("rocksdb_free",
			FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

	private Native() {
		// no instances
	}

	/**
	 * Creates a pre-zeroed error holder in the given arena.
	 * Use this for RocksDB C calls that take {@code char** errptr}.
	 */
	public static MemorySegment errHolder(Arena arena) {
		MemorySegment holder = arena.allocate(ValueLayout.ADDRESS);
		holder.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
		return holder;
	}

	/**
	 * Copies {@code bytes} into a new native memory segment allocated from {@code arena}.
	 * No null-terminator is appended; use the byte length when passing to C functions.
	 */
	public static MemorySegment toNative(Arena arena, byte[] bytes) {
		MemorySegment seg = arena.allocate(bytes.length);
		// TODO: check if this is better seg.copyFrom(MemorySegment.ofArray(bytes));
		MemorySegment.copy(bytes, 0, seg, ValueLayout.JAVA_BYTE, 0, bytes.length);
		return seg;
	}

	/**
	 * Checks if the error holder contains a non-NULL pointer.
	 * If so, throws a {@link RocksDBException} and frees the C string.
	 */
	public static void checkError(MemorySegment errHolder) {
		MemorySegment errPtr = errHolder.get(ValueLayout.ADDRESS, 0);
		if (!MemorySegment.NULL.equals(errPtr)) {
			String msg = errPtr.reinterpret(Long.MAX_VALUE).getString(0);
			try {
				MH_FREE.invokeExact(errPtr);
			} catch (Throwable ignored) {
				// Best effort to free
			}
			throw new RocksDBException(msg);
		}
	}

	/**
	 * Silently executes a close action, discarding any exception.
	 * Use this in all {@code close()} implementations — destructors must not throw.
	 * TODO: calling multiple times this for same object could trigger undefined behavior and other problems like heap corruptions or SIGSEGV etc
	 */
	public static void closeQuietly(MethodHandle destructor, MemorySegment ptr) {
		try {
			destructor.invokeExact(ptr);
		} catch (Throwable ignored) {
		}
	}

	public static void closeQuietly(MethodHandle destructor, MemorySegment ptr1, MemorySegment ptr2) {
		try {
			destructor.invokeExact(ptr1, ptr2);
		} catch (Throwable ignored) {
		}
	}

	// new stuff, human-generated
	private static final Arena ARENA_ERROR = Arena.ofAuto();

	// this is needed to allocate a char** to let rocksdb fill up the error
	public static final Pool<MemorySegment> ERROR = new CachedBlockingPool<>(
			new BlockingPool<>(100, () -> {
				MemorySegment error = ARENA_ERROR.allocate(ValueLayout.ADDRESS);
				error.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
				return error;
			})
	);

}
