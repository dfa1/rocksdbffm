package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_compactoptions_t`.
///
/// Used with [RocksDB#compactRange(CompactOptions, byte\[\], byte\[\])].
///
/// ```
/// try (var opts = new CompactOptions().setChangeLevel(true).setTargetLevel(2)) {
///     db.compactRange(opts, null, null);
/// }
/// ```
public final class CompactOptions extends NativeObject {

	// rocksdb_compactoptions_create(void) -> rocksdb_compactoptions_t*
	private static final MethodHandle MH_CREATE;
	// rocksdb_compactoptions_destroy(rocksdb_compactoptions_t*) -> void
	private static final MethodHandle MH_DESTROY;
	// rocksdb_compactoptions_set_exclusive_manual_compaction(rocksdb_compactoptions_t*, unsigned char) -> void
	private static final MethodHandle MH_SET_EXCLUSIVE;
	// rocksdb_compactoptions_get_exclusive_manual_compaction(rocksdb_compactoptions_t*) -> unsigned char
	private static final MethodHandle MH_GET_EXCLUSIVE;
	// rocksdb_compactoptions_set_bottommost_level_compaction(rocksdb_compactoptions_t*, unsigned char) -> void
	private static final MethodHandle MH_SET_BOTTOMMOST;
	// rocksdb_compactoptions_get_bottommost_level_compaction(rocksdb_compactoptions_t*) -> unsigned char
	private static final MethodHandle MH_GET_BOTTOMMOST;
	// rocksdb_compactoptions_set_change_level(rocksdb_compactoptions_t*, unsigned char) -> void
	private static final MethodHandle MH_SET_CHANGE_LEVEL;
	// rocksdb_compactoptions_get_change_level(rocksdb_compactoptions_t*) -> unsigned char
	private static final MethodHandle MH_GET_CHANGE_LEVEL;
	// rocksdb_compactoptions_set_target_level(rocksdb_compactoptions_t*, int) -> void
	private static final MethodHandle MH_SET_TARGET_LEVEL;
	// rocksdb_compactoptions_get_target_level(rocksdb_compactoptions_t*) -> int
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

	/// If `true`, no other manual compaction will run in parallel.
	/// Default: `true`.
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

	/// If `true`, the compaction will compact all data at the bottommost level.
	/// Default: `false`.
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

	/// If `true`, compacted output will be moved to the level set by
	/// [#setTargetLevel(int)].
	/// Default: `false`.
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

	/// Target output level for the compaction when [#setChangeLevel(boolean)] is
	/// `true`. `-1` means the bottommost level. Default: `-1`.
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
