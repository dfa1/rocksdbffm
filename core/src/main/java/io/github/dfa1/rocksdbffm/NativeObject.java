package io.github.dfa1.rocksdbffm;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for all Java wrappers around native RocksDB pointers.
 *
 * <p>Holds the native pointer in an {@link AtomicReference}. On {@link #close()},
 * the reference is atomically swapped to {@link MemorySegment#NULL}, ensuring
 * {@link #tryClose(MemorySegment)} is called exactly once even under
 * concurrent or repeated close() calls.
 */
public abstract class NativeObject implements AutoCloseable {

	private final AtomicReference<MemorySegment> owningPointer;

	protected NativeObject(MemorySegment owningPointer) {
		this.owningPointer = new AtomicReference<>(owningPointer);
	}

	/**
	 * Returns the current native pointer. May be {@link MemorySegment#NULL} after close.
	 */
	public final MemorySegment ptr() {
		return owningPointer.get();
	}

	@Override
	public final void close() {
		MemorySegment ptr = owningPointer.getAndSet(MemorySegment.NULL);
		if (!MemorySegment.NULL.equals(ptr)) {
			try {
				tryClose(ptr);
			} catch (Throwable throwable) {
				// ignored
			}
		}
	}

	/**
	 * Called exactly once with the non-NULL pointer when this object is closed.
	 * Implementations must release the native resource.
	 */
	protected abstract void tryClose(MemorySegment ptr) throws Throwable;
}
