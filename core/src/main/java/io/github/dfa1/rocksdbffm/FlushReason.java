package io.github.dfa1.rocksdbffm;

/// Why a memtable flush was triggered, reported on [FlushJobInfo#flushReason()].
///
/// Integer values match `rocksdb::FlushReason` (`rocksdb/listener.h`), surfaced through
/// `rocksdb_flushjobinfo_flush_reason()`.
public enum FlushReason {

	/// Catch-all for flush reasons not otherwise listed here.
	OTHERS,

	/// Triggered by `DB::GetLiveFiles()`.
	GET_LIVE_FILES,

	/// Triggered while the database is shutting down.
	SHUT_DOWN,

	/// Triggered by ingesting an external SST file.
	EXTERNAL_FILE_INGESTION,

	/// Triggered by a manual compaction.
	MANUAL_COMPACTION,

	/// Triggered by the write buffer manager reclaiming memory.
	WRITE_BUFFER_MANAGER,

	/// Triggered because a memtable's write buffer became full.
	WRITE_BUFFER_FULL,

	/// Used internally by RocksDB's own tests.
	TEST,

	/// Triggered by a file-deletion request.
	DELETE_FILES,

	/// Triggered automatically by the compaction pipeline.
	AUTO_COMPACTION,

	/// Triggered by an explicit `Flush()` call.
	MANUAL_FLUSH,

	/// Triggered while recovering from a background error.
	ERROR_RECOVERY,

	/// Same as [#ERROR_RECOVERY], but the memtable is not switched, to avoid
	/// accumulating many small immutable memtables.
	ERROR_RECOVERY_RETRY_FLUSH,

	/// Triggered because the write-ahead log became full.
	WAL_FULL,

	/// Triggered to catch up after a background error was recovered from; the memtable is not
	/// switched.
	CATCH_UP_AFTER_ERROR_RECOVERY,

	/// Triggered because a memtable held too many range deletions.
	MEMTABLE_MAX_RANGE_DELETIONS;

	static FlushReason fromValue(int value) {
		return switch (value) {
			case 0x00 -> OTHERS;
			case 0x01 -> GET_LIVE_FILES;
			case 0x02 -> SHUT_DOWN;
			case 0x03 -> EXTERNAL_FILE_INGESTION;
			case 0x04 -> MANUAL_COMPACTION;
			case 0x05 -> WRITE_BUFFER_MANAGER;
			case 0x06 -> WRITE_BUFFER_FULL;
			case 0x07 -> TEST;
			case 0x08 -> DELETE_FILES;
			case 0x09 -> AUTO_COMPACTION;
			case 0x0a -> MANUAL_FLUSH;
			case 0x0b -> ERROR_RECOVERY;
			case 0x0c -> ERROR_RECOVERY_RETRY_FLUSH;
			case 0x0d -> WAL_FULL;
			case 0x0e -> CATCH_UP_AFTER_ERROR_RECOVERY;
			case 0x0f -> MEMTABLE_MAX_RANGE_DELETIONS;
			default -> throw new IllegalArgumentException("Unknown flush reason value: " + value);
		};
	}
}
