package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_env_t`.
///
/// An Env encapsulates system interactions (file I/O, threading, time).
/// Required to create an [SstFileManager].
///
/// ```
/// try (Env env = Env.defaultEnv();
///      SstFileManager sfm = SstFileManager.create(env)) {
///     ...
/// }
/// ```
public final class Env extends NativeObject {

	/// `rocksdb_env_t* rocksdb_create_default_env(void);`
	private static final MethodHandle MH_CREATE_DEFAULT;
	/// `rocksdb_env_t* rocksdb_create_mem_env(void);`
	private static final MethodHandle MH_CREATE_MEM;
	/// `void rocksdb_env_destroy(rocksdb_env_t*);`
	private static final MethodHandle MH_DESTROY;
	/// `void rocksdb_env_set_background_threads(rocksdb_env_t* env, int n);`
	private static final MethodHandle MH_SET_BACKGROUND_THREADS;
	/// `int rocksdb_env_get_background_threads(rocksdb_env_t* env);`
	private static final MethodHandle MH_GET_BACKGROUND_THREADS;
	/// `void rocksdb_env_set_high_priority_background_threads(rocksdb_env_t* env, int n);`
	private static final MethodHandle MH_SET_HIGH_PRIORITY_BACKGROUND_THREADS;
	/// `int rocksdb_env_get_high_priority_background_threads(rocksdb_env_t* env);`
	private static final MethodHandle MH_GET_HIGH_PRIORITY_BACKGROUND_THREADS;

	static {
		MH_CREATE_DEFAULT = NativeLibrary.lookup("rocksdb_create_default_env",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_CREATE_MEM = NativeLibrary.lookup("rocksdb_create_mem_env",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = NativeLibrary.lookup("rocksdb_env_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SET_BACKGROUND_THREADS = NativeLibrary.lookup("rocksdb_env_set_background_threads",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_BACKGROUND_THREADS = NativeLibrary.lookup("rocksdb_env_get_background_threads",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_SET_HIGH_PRIORITY_BACKGROUND_THREADS = NativeLibrary.lookup(
				"rocksdb_env_set_high_priority_background_threads",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_HIGH_PRIORITY_BACKGROUND_THREADS = NativeLibrary.lookup(
				"rocksdb_env_get_high_priority_background_threads",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
	}

	private Env(MemorySegment ptr) {
		super(ptr);
	}

	/// Creates a wrapper around the POSIX default environment.
	///
	/// The underlying `Env::Default()` singleton is never deleted, but the
	/// wrapper object returned here must be closed when no longer needed.
	///
	/// @return a new [Env] wrapping the default environment; caller must close it
	public static Env defaultEnv() {
		try {
			return new Env((MemorySegment) MH_CREATE_DEFAULT.invokeExact());
		} catch (Throwable t) {
			throw new RocksDBException("Env.defaultEnv failed", t);
		}
	}

	/// Creates an in-memory environment useful for testing.
	///
	/// @return a new in-memory [Env]; caller must close it
	public static Env memEnv() {
		try {
			return new Env((MemorySegment) MH_CREATE_MEM.invokeExact());
		} catch (Throwable t) {
			throw new RocksDBException("Env.memEnv failed", t);
		}
	}

	/// Sets the number of low-priority background threads for this env.
	///
	/// @param n number of low-priority background threads
	/// @return `this` for chaining
	public Env setBackgroundThreads(int n) {
		try {
			MH_SET_BACKGROUND_THREADS.invokeExact(ptr(), n);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setBackgroundThreads failed", t);
		}
	}

	/// Returns the number of low-priority background threads.
	///
	/// @return current low-priority thread count
	public int getBackgroundThreads() {
		try {
			return (int) MH_GET_BACKGROUND_THREADS.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getBackgroundThreads failed", t);
		}
	}

	/// Sets the number of high-priority background threads (used for compaction).
	///
	/// @param n number of high-priority background threads
	/// @return `this` for chaining
	public Env setHighPriorityBackgroundThreads(int n) {
		try {
			MH_SET_HIGH_PRIORITY_BACKGROUND_THREADS.invokeExact(ptr(), n);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setHighPriorityBackgroundThreads failed", t);
		}
	}

	/// Returns the number of high-priority background threads.
	///
	/// @return current high-priority thread count
	public int getHighPriorityBackgroundThreads() {
		try {
			return (int) MH_GET_HIGH_PRIORITY_BACKGROUND_THREADS.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getHighPriorityBackgroundThreads failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
