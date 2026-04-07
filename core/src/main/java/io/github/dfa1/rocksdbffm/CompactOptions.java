package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for {@code rocksdb_compactoptions_t}.
 *
 * <p>Used with {@link RocksDB#compactRange(CompactOptions, byte[], byte[])}.
 *
 * <pre>{@code
 * try (var opts = new CompactOptions().setChangeLevel(true).setTargetLevel(2)) {
 *     db.compactRange(opts, null, null);
 * }
 * }</pre>
 */
public final class CompactOptions extends NativeObject {

	private static final MethodHandle MH_CREATE;
	private static final MethodHandle MH_DESTROY;
	private static final MethodHandle MH_SET_EXCLUSIVE;
	private static final MethodHandle MH_GET_EXCLUSIVE;
	private static final MethodHandle MH_SET_BOTTOMMOST;
	private static final MethodHandle MH_GET_BOTTOMMOST;
	private static final MethodHandle MH_SET_CHANGE_LEVEL;
	private static final MethodHandle MH_GET_CHANGE_LEVEL;
	private static final MethodHandle MH_SET_TARGET_LEVEL;
	private static final MethodHandle MH_GET_TARGET_LEVEL;

	static {
		MH_CREATE = RocksDB.lookup("rocksdb_compactoptions_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = RocksDB.lookup("rocksdb_compactoptions_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SET_EXCLUSIVE = RocksDB.lookup(
				"rocksdb_compactoptions_set_exclusive_manual_compaction",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_GET_EXCLUSIVE = RocksDB.lookup(
				"rocksdb_compactoptions_get_exclusive_manual_compaction",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_SET_BOTTOMMOST = RocksDB.lookup(
				"rocksdb_compactoptions_set_bottommost_level_compaction",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_GET_BOTTOMMOST = RocksDB.lookup(
				"rocksdb_compactoptions_get_bottommost_level_compaction",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_SET_CHANGE_LEVEL = RocksDB.lookup(
				"rocksdb_compactoptions_set_change_level",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_GET_CHANGE_LEVEL = RocksDB.lookup(
				"rocksdb_compactoptions_get_change_level",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_SET_TARGET_LEVEL = RocksDB.lookup(
				"rocksdb_compactoptions_set_target_level",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_TARGET_LEVEL = RocksDB.lookup(
				"rocksdb_compactoptions_get_target_level",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
	}

	private CompactOptions(MemorySegment ptr) {
		super(ptr);
	}

	public static CompactOptions newCompactOptions() {
		MemorySegment result;
		try {
			result = (MemorySegment) MH_CREATE.invokeExact();
		} catch (Throwable e) {
			throw new RocksDBException(e.getMessage());
		}
		return new CompactOptions(result);
	}

	/**
	 * If {@code true}, no other manual compaction will run in parallel.
	 * Default: {@code true}.
	 */
	public CompactOptions setExclusiveManualCompaction(boolean value) {
		try {
			MH_SET_EXCLUSIVE.invokeExact(ptr(), value ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw new RocksDBException("setExclusiveManualCompaction failed", t);
		}
		return this;
	}

	public boolean isExclusiveManualCompaction() {
		try {
			return (byte) MH_GET_EXCLUSIVE.invokeExact(ptr()) != 0;
		} catch (Throwable t) {
			throw new RocksDBException("isExclusiveManualCompaction failed", t);
		}
	}

	/**
	 * If {@code true}, the compaction will compact all data at the bottommost level.
	 * Default: {@code false}.
	 */
	public CompactOptions setBottommostLevelCompaction(boolean value) {
		try {
			MH_SET_BOTTOMMOST.invokeExact(ptr(), value ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw new RocksDBException("setBottommostLevelCompaction failed", t);
		}
		return this;
	}

	public boolean isBottommostLevelCompaction() {
		try {
			return (byte) MH_GET_BOTTOMMOST.invokeExact(ptr()) != 0;
		} catch (Throwable t) {
			throw new RocksDBException("isBottommostLevelCompaction failed", t);
		}
	}

	/**
	 * If {@code true}, compacted output will be moved to the level set by
	 * {@link #setTargetLevel(int)}.
	 * Default: {@code false}.
	 */
	public CompactOptions setChangeLevel(boolean value) {
		try {
			MH_SET_CHANGE_LEVEL.invokeExact(ptr(), value ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw new RocksDBException("setChangeLevel failed", t);
		}
		return this;
	}

	public boolean isChangeLevel() {
		try {
			return (byte) MH_GET_CHANGE_LEVEL.invokeExact(ptr()) != 0;
		} catch (Throwable t) {
			throw new RocksDBException("isChangeLevel failed", t);
		}
	}

	/**
	 * Target output level for the compaction when {@link #setChangeLevel(boolean)} is
	 * {@code true}. {@code -1} means the bottommost level. Default: {@code -1}.
	 */
	public CompactOptions setTargetLevel(int level) {
		try {
			MH_SET_TARGET_LEVEL.invokeExact(ptr(), level);
		} catch (Throwable t) {
			throw new RocksDBException("setTargetLevel failed", t);
		}
		return this;
	}

	public int getTargetLevel() {
		try {
			return (int) MH_GET_TARGET_LEVEL.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getTargetLevel failed", t);
		}
	}

	@Override
	public void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
