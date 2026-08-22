package io.github.dfa1.rocksdbffm;

/// Receives RocksDB's internal lifecycle events: flushes, compactions, external file ingestion,
/// background errors, write stalls, and memtable seals.
///
/// Implement only the methods you need — every method has a no-op default. This mirrors most of
/// `rocksdb_eventlistener_create()`'s C callback surface (`rocksdb/c.h`); the two subcompaction
/// callbacks are deliberately not exposed here. Attach an instance via
/// [Options#addEventListener(EventNotifier)]; it may be called multiple times to register several
/// listeners.
///
/// Every callback may run on one of RocksDB's internal background threads, so implementations
/// must be thread-safe. A callback must not throw: an escaping exception is caught and logged,
/// never propagated back into RocksDB or across the native boundary.
///
/// The `*Info` parameters are zero-copy views into memory RocksDB owns for the duration of the
/// call only — never retain one past the method that received it.
///
/// ```
/// try (var opts = Options.newOptions().setCreateIfMissing(true)) {
///     opts.addEventListener(new EventNotifier() {
///         public void onFlushCompleted(FlushJobInfo info) {
///             System.out.println("flushed " + info.filePath());
///         }
///     });
///     var db = RocksDB.openReadWrite(opts, dbPath);
/// }
/// ```
public interface EventNotifier {

	/// Called before a memtable flush starts.
	///
	/// @param info details of the flush about to run
	default void onFlushBegin(FlushJobInfo info) {
	}

	/// Called after a memtable flush completes.
	///
	/// @param info details of the completed flush
	default void onFlushCompleted(FlushJobInfo info) {
	}

	/// Called before a compaction starts.
	///
	/// @param info details of the compaction about to run
	default void onCompactionBegin(CompactionJobInfo info) {
	}

	/// Called after a compaction completes.
	///
	/// @param info details of the completed compaction
	default void onCompactionCompleted(CompactionJobInfo info) {
	}

	/// Called after an external SST file has been ingested.
	///
	/// @param info details of the ingested file
	default void onExternalFileIngested(ExternalFileIngestionInfo info) {
	}

	/// Called when a background flush, compaction, or write encounters an unrecoverable error.
	///
	/// @param reason which subsystem reported the error
	/// @param error  the error RocksDB recorded
	default void onBackgroundError(BackgroundErrorReason reason, RocksDBException error) {
	}

	/// Called when RocksDB's write controller changes the database's write stall state.
	///
	/// @param info the new and previous stall conditions
	default void onStallConditionsChanged(WriteStallInfo info) {
	}

	/// Called when a memtable becomes immutable (sealed) and is queued for flush.
	///
	/// @param info details of the sealed memtable
	default void onMemTableSealed(MemTableInfo info) {
	}
}
