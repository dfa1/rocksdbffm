package io.github.dfa1.rocksdbffm;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicReference;

/// Base class for all Java wrappers around native RocksDB pointers.
///
/// Holds the native pointer in an [AtomicReference]. On [#close()],
/// the reference is atomically swapped to [MemorySegment#NULL], ensuring
/// [#tryClose(MemorySegment)] is called exactly once even under
/// concurrent or repeated close() calls.
public abstract class NativeObject implements AutoCloseable {

	private final AtomicReference<MemorySegment> owningPointer;

	/// Initialises this wrapper with the given native pointer.
	///
	/// @param owningPointer the non-NULL native pointer this object now owns
	protected NativeObject(MemorySegment owningPointer) {
		this.owningPointer = new AtomicReference<>(owningPointer);
	}

	/// Returns the current native pointer.
	///
	/// @return the live native pointer
	/// @throws IllegalStateException if this object has been closed or its ownership transferred
	public final MemorySegment ptr() {
		MemorySegment p = owningPointer.get();
		if (MemorySegment.NULL.equals(p)) {
			throw new IllegalStateException("native object is closed or ownership has been transferred");
		}
		return p;
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

	///
	/// To be invoked manually when this object does not have anymore the ownership of the pointer.
	/// How to know when to call this?
	/// - RocksDB C API source (db/c.cc) — look at what rocksdb_block_based_options_destroy does: if it calls delete options->rep.filter_policy, ownership was transferred.
	/// - RocksDB documentation — the C API docs sometimes state it explicitly.
	/// - The Java JNI bindings (RocksDB official Java library) — they've already solved this; look at how BlockBasedTableConfig handles filter policy lifecycle.
	void transferOwnership() {
		owningPointer.set(MemorySegment.NULL);
	}

	/// Called exactly once with the non-NULL pointer when this object is closed.
	/// Implementations must release the native resource.
	///
	/// @param ptr the non-NULL native pointer to release
	/// @throws Throwable if the native destroy call fails (exception is silently swallowed by [#close()])
	protected abstract void tryClose(MemorySegment ptr) throws Throwable;
}
