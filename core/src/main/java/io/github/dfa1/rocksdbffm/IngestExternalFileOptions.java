package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_ingestexternalfileoptions_t.
 *
 * <p>Controls the behaviour of {@link RocksDB#ingestExternalFile}.
 *
 * <pre>{@code
 * try (var opts = new IngestExternalFileOptions().setMoveFiles(true)) {
 *     db.ingestExternalFile(List.of(sstPath), opts);
 * }
 * }</pre>
 */
public final class IngestExternalFileOptions implements AutoCloseable {

	private static final MethodHandle MH_CREATE;
	private static final MethodHandle MH_DESTROY;
	private static final MethodHandle MH_SET_MOVE_FILES;
	private static final MethodHandle MH_SET_SNAPSHOT_CONSISTENCY;
	private static final MethodHandle MH_SET_ALLOW_GLOBAL_SEQNO;
	private static final MethodHandle MH_SET_ALLOW_BLOCKING_FLUSH;
	private static final MethodHandle MH_SET_INGEST_BEHIND;
	private static final MethodHandle MH_SET_FAIL_IF_NOT_BOTTOMMOST_LEVEL;

	static {
		MH_CREATE = RocksDB.lookup("rocksdb_ingestexternalfileoptions_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = RocksDB.lookup("rocksdb_ingestexternalfileoptions_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		// void rocksdb_ingestexternalfileoptions_set_move_files(opts*, unsigned char)
		MH_SET_MOVE_FILES = RocksDB.lookup("rocksdb_ingestexternalfileoptions_set_move_files",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		// void rocksdb_ingestexternalfileoptions_set_snapshot_consistency(opts*, unsigned char)
		MH_SET_SNAPSHOT_CONSISTENCY = RocksDB.lookup("rocksdb_ingestexternalfileoptions_set_snapshot_consistency",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		// void rocksdb_ingestexternalfileoptions_set_allow_global_seqno(opts*, unsigned char)
		MH_SET_ALLOW_GLOBAL_SEQNO = RocksDB.lookup("rocksdb_ingestexternalfileoptions_set_allow_global_seqno",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		// void rocksdb_ingestexternalfileoptions_set_allow_blocking_flush(opts*, unsigned char)
		MH_SET_ALLOW_BLOCKING_FLUSH = RocksDB.lookup("rocksdb_ingestexternalfileoptions_set_allow_blocking_flush",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		// void rocksdb_ingestexternalfileoptions_set_ingest_behind(opts*, unsigned char)
		MH_SET_INGEST_BEHIND = RocksDB.lookup("rocksdb_ingestexternalfileoptions_set_ingest_behind",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		// void rocksdb_ingestexternalfileoptions_set_fail_if_not_bottommost_level(opts*, unsigned char)
		MH_SET_FAIL_IF_NOT_BOTTOMMOST_LEVEL = RocksDB.lookup(
				"rocksdb_ingestexternalfileoptions_set_fail_if_not_bottommost_level",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));
	}

	final MemorySegment ptr;

	public IngestExternalFileOptions() {
		try {
			this.ptr = (MemorySegment) MH_CREATE.invokeExact();
		} catch (Throwable t) {
			throw new RocksDBException("ingestexternalfileoptions create failed", t);
		}
	}

	/**
	 * If {@code true}, the SST files are moved rather than copied into the DB directory.
	 *
	 * @return {@code this} for chaining
	 */
	public IngestExternalFileOptions setMoveFiles(boolean moveFiles) {
		try {
			MH_SET_MOVE_FILES.invokeExact(ptr, moveFiles ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("ingestexternalfileoptions setMoveFiles failed", t);
		}
	}

	/**
	 * If {@code true} (default), snapshot consistency is enforced during ingest.
	 *
	 * @return {@code this} for chaining
	 */
	public IngestExternalFileOptions setSnapshotConsistency(boolean snapshotConsistency) {
		try {
			MH_SET_SNAPSHOT_CONSISTENCY.invokeExact(ptr, snapshotConsistency ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("ingestexternalfileoptions setSnapshotConsistency failed", t);
		}
	}

	/**
	 * If {@code true} (default), allows assigning a global sequence number to ingested files.
	 *
	 * @return {@code this} for chaining
	 */
	public IngestExternalFileOptions setAllowGlobalSeqno(boolean allowGlobalSeqno) {
		try {
			MH_SET_ALLOW_GLOBAL_SEQNO.invokeExact(ptr, allowGlobalSeqno ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("ingestexternalfileoptions setAllowGlobalSeqno failed", t);
		}
	}

	/**
	 * If {@code true} (default), allows a blocking flush before ingest if needed.
	 *
	 * @return {@code this} for chaining
	 */
	public IngestExternalFileOptions setAllowBlockingFlush(boolean allowBlockingFlush) {
		try {
			MH_SET_ALLOW_BLOCKING_FLUSH.invokeExact(ptr, allowBlockingFlush ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("ingestexternalfileoptions setAllowBlockingFlush failed", t);
		}
	}

	/**
	 * If {@code true}, ingest files behind existing data (at the bottommost level).
	 * Requires {@code allow_ingest_behind} to be set on the DB options.
	 *
	 * @return {@code this} for chaining
	 */
	public IngestExternalFileOptions setIngestBehind(boolean ingestBehind) {
		try {
			MH_SET_INGEST_BEHIND.invokeExact(ptr, ingestBehind ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("ingestexternalfileoptions setIngestBehind failed", t);
		}
	}

	/**
	 * If {@code true}, fails if the file cannot be placed at the bottommost level.
	 *
	 * @return {@code this} for chaining
	 */
	public IngestExternalFileOptions setFailIfNotBottommostLevel(boolean failIfNotBottommostLevel) {
		try {
			MH_SET_FAIL_IF_NOT_BOTTOMMOST_LEVEL.invokeExact(ptr, failIfNotBottommostLevel ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("ingestexternalfileoptions setFailIfNotBottommostLevel failed", t);
		}
	}

	@Override
	public void close() {
		try {
			MH_DESTROY.invokeExact(ptr);
		} catch (Throwable t) {
			throw new RocksDBException("ingestexternalfileoptions destroy failed", t);
		}
	}
}
