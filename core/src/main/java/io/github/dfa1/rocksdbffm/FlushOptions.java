package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_flushoptions_t.
 *
 * <p>Controls the behaviour of {@link RocksDB#flush(FlushOptions)} and
 * {@link TransactionDB#flush(FlushOptions)}.
 *
 * <pre>{@code
 * try (FlushOptions fo = FlushOptions.newFlushOptions().setWait(true)) {
 *     db.flush(fo);
 * }
 * }</pre>
 */
public final class FlushOptions extends NativeObject {

	// rocksdb_flushoptions_create(void);
	private static final MethodHandle MH_CREATE;
	// rocksdb_flushoptions_destroy(rocksdb_flushoptions_t*);
	private static final MethodHandle MH_DESTROY;
	// rocksdb_flushoptions_set_wait(rocksdb_flushoptions_t*, unsigned char);
	private static final MethodHandle MH_SET_WAIT;
	// rocksdb_flushoptions_get_wait(rocksdb_flushoptions_t*);
	private static final MethodHandle MH_GET_WAIT;

	static {
		MH_CREATE = RocksDB.lookup("rocksdb_flushoptions_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = RocksDB.lookup("rocksdb_flushoptions_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		// void rocksdb_flushoptions_set_wait(opts*, unsigned char)
		MH_SET_WAIT = RocksDB.lookup("rocksdb_flushoptions_set_wait",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		// unsigned char rocksdb_flushoptions_get_wait(opts*)
		MH_GET_WAIT = RocksDB.lookup("rocksdb_flushoptions_get_wait",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));
	}

	private FlushOptions(MemorySegment ptr) {
		super(ptr);
	}

	/**
	 * Creates FlushOptions with {@code wait = true} (the default).
	 */
	public static FlushOptions newFlushOptions() {
		try {
			return new FlushOptions((MemorySegment) MH_CREATE.invokeExact());
		} catch (Throwable t) {
			throw new RocksDBException("flushoptions create failed", t);
		}
	}

	/**
	 * If {@code true} (default), {@code flush()} blocks until the memtable flush
	 * completes. If {@code false}, the flush is submitted asynchronously.
	 *
	 * @return {@code this} for chaining
	 */
	public FlushOptions setWait(boolean wait) {
		try {
			MH_SET_WAIT.invokeExact(ptr(), wait ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("flushoptions setWait failed", t);
		}
	}

	/**
	 * Returns whether flush waits for completion.
	 */
	public boolean isWait() {
		try {
			return ((byte) MH_GET_WAIT.invokeExact(ptr())) != 0;
		} catch (Throwable t) {
			throw new RocksDBException("flushoptions getWait failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
