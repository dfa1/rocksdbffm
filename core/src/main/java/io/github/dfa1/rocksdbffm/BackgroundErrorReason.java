package io.github.dfa1.rocksdbffm;

/// Which internal subsystem reported the background error passed to
/// [EventNotifier#onBackgroundError(BackgroundErrorReason, RocksDBException)].
///
/// Integer values match `rocksdb::BackgroundErrorReason` (`rocksdb/listener.h`), surfaced as the
/// `uint32_t reason` argument of `on_background_error_cb`.
public enum BackgroundErrorReason {

	/// The error occurred during a flush.
	FLUSH,

	/// The error occurred during a compaction.
	COMPACTION,

	/// The error occurred while invoking a write callback.
	WRITE_CALLBACK,

	/// The error occurred while inserting into a memtable.
	MEMTABLE,

	/// The error occurred while writing to the MANIFEST.
	MANIFEST_WRITE,

	/// The error occurred during a flush that does not write to the WAL.
	FLUSH_NO_WAL,

	/// The error occurred while writing to the MANIFEST during a WAL-less flush.
	MANIFEST_WRITE_NO_WAL,

	/// The error occurred while asynchronously opening a file.
	ASYNC_FILE_OPEN;

	static BackgroundErrorReason fromValue(int value) {
		return switch (value) {
			case 0 -> FLUSH;
			case 1 -> COMPACTION;
			case 2 -> WRITE_CALLBACK;
			case 3 -> MEMTABLE;
			case 4 -> MANIFEST_WRITE;
			case 5 -> FLUSH_NO_WAL;
			case 6 -> MANIFEST_WRITE_NO_WAL;
			case 7 -> ASYNC_FILE_OPEN;
			default -> throw new IllegalArgumentException("Unknown background error reason value: " + value);
		};
	}
}
