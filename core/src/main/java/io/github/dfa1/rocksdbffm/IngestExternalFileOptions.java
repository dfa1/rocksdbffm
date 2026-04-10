package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_ingestexternalfileoptions_t`.
///
/// Controls the behavior of [RocksDB#ingestExternalFile].
///
/// ```
/// try (var opts = IngestExternalFileOptions.newIngestExternalFileOptions().setMoveFiles(true)) {
///     db.ingestExternalFile(List.of(sstPath), opts);
/// }
/// ```
public final class IngestExternalFileOptions extends NativeObject {

	/// `rocksdb_ingestexternalfileoptions_t* rocksdb_ingestexternalfileoptions_create(void);`
	private static final MethodHandle MH_CREATE;
	/// `void rocksdb_ingestexternalfileoptions_destroy(rocksdb_ingestexternalfileoptions_t* opt);`
	private static final MethodHandle MH_DESTROY;
	/// `void rocksdb_ingestexternalfileoptions_set_move_files(rocksdb_ingestexternalfileoptions_t* opt, unsigned char move_files);`
	private static final MethodHandle MH_SET_MOVE_FILES;
	/// `void rocksdb_ingestexternalfileoptions_set_snapshot_consistency(rocksdb_ingestexternalfileoptions_t* opt, unsigned char snapshot_consistency);`
	private static final MethodHandle MH_SET_SNAPSHOT_CONSISTENCY;
	/// `void rocksdb_ingestexternalfileoptions_set_allow_global_seqno(rocksdb_ingestexternalfileoptions_t* opt, unsigned char allow_global_seqno);`
	private static final MethodHandle MH_SET_ALLOW_GLOBAL_SEQNO;
	/// `void rocksdb_ingestexternalfileoptions_set_allow_blocking_flush(rocksdb_ingestexternalfileoptions_t* opt, unsigned char allow_blocking_flush);`
	private static final MethodHandle MH_SET_ALLOW_BLOCKING_FLUSH;
	/// `void rocksdb_ingestexternalfileoptions_set_ingest_behind(rocksdb_ingestexternalfileoptions_t* opt, unsigned char ingest_behind);`
	private static final MethodHandle MH_SET_INGEST_BEHIND;
	/// `void rocksdb_ingestexternalfileoptions_set_fail_if_not_bottommost_level(rocksdb_ingestexternalfileoptions_t* opt, unsigned char fail_if_not_bottommost_level);`
	private static final MethodHandle MH_SET_FAIL_IF_NOT_BOTTOMMOST_LEVEL;

	static {
		MH_CREATE = NativeLibrary.lookup("rocksdb_ingestexternalfileoptions_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = NativeLibrary.lookup("rocksdb_ingestexternalfileoptions_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SET_MOVE_FILES = NativeLibrary.lookup("rocksdb_ingestexternalfileoptions_set_move_files",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_SET_SNAPSHOT_CONSISTENCY = NativeLibrary.lookup("rocksdb_ingestexternalfileoptions_set_snapshot_consistency",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_SET_ALLOW_GLOBAL_SEQNO = NativeLibrary.lookup("rocksdb_ingestexternalfileoptions_set_allow_global_seqno",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_SET_ALLOW_BLOCKING_FLUSH = NativeLibrary.lookup("rocksdb_ingestexternalfileoptions_set_allow_blocking_flush",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_SET_INGEST_BEHIND = NativeLibrary.lookup("rocksdb_ingestexternalfileoptions_set_ingest_behind",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_SET_FAIL_IF_NOT_BOTTOMMOST_LEVEL = NativeLibrary.lookup(
				"rocksdb_ingestexternalfileoptions_set_fail_if_not_bottommost_level",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));
	}

	private IngestExternalFileOptions(MemorySegment ptr) {
		super(ptr);
	}

	public static IngestExternalFileOptions newIngestExternalFileOptions() {
		try {
			return new IngestExternalFileOptions((MemorySegment) MH_CREATE.invokeExact());
		} catch (Throwable t) {
			throw new RocksDBException("ingestexternalfileoptions create failed", t);
		}
	}

	/// If `true`, the SST files are moved rather than copied into the DB directory.
	///
	/// @return `this` for chaining
	public IngestExternalFileOptions setMoveFiles(boolean moveFiles) {
		try {
			MH_SET_MOVE_FILES.invokeExact(ptr(), moveFiles ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("ingestexternalfileoptions setMoveFiles failed", t);
		}
	}

	/// If `true` (default), snapshot consistency is enforced during ingest.
	///
	/// @return `this` for chaining
	public IngestExternalFileOptions setSnapshotConsistency(boolean snapshotConsistency) {
		try {
			MH_SET_SNAPSHOT_CONSISTENCY.invokeExact(ptr(), snapshotConsistency ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("ingestexternalfileoptions setSnapshotConsistency failed", t);
		}
	}

	/// If `true` (default), allows assigning a global sequence number to ingested files.
	///
	/// @return `this` for chaining
	public IngestExternalFileOptions setAllowGlobalSeqno(boolean allowGlobalSeqno) {
		try {
			MH_SET_ALLOW_GLOBAL_SEQNO.invokeExact(ptr(), allowGlobalSeqno ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("ingestexternalfileoptions setAllowGlobalSeqno failed", t);
		}
	}

	/// If `true` (default), allows a blocking flush before ingest if needed.
	///
	/// @return `this` for chaining
	public IngestExternalFileOptions setAllowBlockingFlush(boolean allowBlockingFlush) {
		try {
			MH_SET_ALLOW_BLOCKING_FLUSH.invokeExact(ptr(), allowBlockingFlush ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("ingestexternalfileoptions setAllowBlockingFlush failed", t);
		}
	}

	/// If `true`, ingest files behind existing data (at the bottommost level).
	/// Requires `allow_ingest_behind` to be set on the DB options.
	///
	/// @return `this` for chaining
	public IngestExternalFileOptions setIngestBehind(boolean ingestBehind) {
		try {
			MH_SET_INGEST_BEHIND.invokeExact(ptr(), ingestBehind ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("ingestexternalfileoptions setIngestBehind failed", t);
		}
	}

	/// If `true`, fails if the file cannot be placed at the bottommost level.
	///
	/// @return `this` for chaining
	public IngestExternalFileOptions setFailIfNotBottommostLevel(boolean failIfNotBottommostLevel) {
		try {
			MH_SET_FAIL_IF_NOT_BOTTOMMOST_LEVEL.invokeExact(ptr(), failIfNotBottommostLevel ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("ingestexternalfileoptions setFailIfNotBottommostLevel failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
