package io.github.dfa1.rocksdbffm;

/// Why a compaction was triggered, reported on [CompactionJobInfo#compactionReason()] and
/// [SubcompactionJobInfo#compactionReason()].
///
/// Integer values match `rocksdb::CompactionReason` (`rocksdb/listener.h`), surfaced through
/// `rocksdb_compactionjobinfo_compaction_reason()` /
/// `rocksdb_subcompactionjobinfo_compaction_reason()`. `kNumOfReasons`, a sentinel counting the
/// number of reasons rather than a real one, is deliberately not mapped here.
public enum CompactionReason {

	/// No specific reason was recorded.
	UNKNOWN,

	/// Level style: number of level-0 files exceeded `level0_file_num_compaction_trigger`.
	LEVEL_L0_FILES_NUM,

	/// Level style: total size of a level exceeded its target size.
	LEVEL_MAX_LEVEL_SIZE,

	/// Universal style: compacting to reduce size amplification.
	UNIVERSAL_SIZE_AMPLIFICATION,

	/// Universal style: compacting to reduce size ratio.
	UNIVERSAL_SIZE_RATIO,

	/// Universal style: number of sorted runs exceeded `level0_file_num_compaction_trigger`.
	UNIVERSAL_SORTED_RUN_NUM,

	/// FIFO style: total size exceeded `max_table_files_size`.
	FIFO_MAX_SIZE,

	/// FIFO style: compacting to reduce the number of files.
	FIFO_REDUCE_NUM_FILES,

	/// FIFO style: files older than the configured TTL interval.
	FIFO_TTL,

	/// Triggered by an explicit `CompactRange()` call.
	MANUAL_COMPACTION,

	/// Triggered by `DB::SuggestCompactRange()`.
	FILES_MARKED_FOR_COMPACTION,

	/// Level style: automatic bottommost-level compaction cleaning up duplicate versions of the
	/// same user key, usually after a snapshot was released.
	BOTTOMMOST_FILES,

	/// Compaction based on TTL.
	TTL,

	/// A flush, internally accounted for as a level-0 compaction.
	FLUSH,

	/// External SST file ingestion, internally accounted for as a compaction.
	EXTERNAL_SST_INGESTION,

	/// Compaction triggered because an SST file exceeded its periodic-compaction age.
	PERIODIC_COMPACTION,

	/// Compaction that moves files to a different temperature tier.
	CHANGE_TEMPERATURE,

	/// Compaction scheduled to force garbage collection of blob files.
	FORCED_BLOB_GC,

	/// Round-robin-policy TTL compaction, functionally similar to [#LEVEL_MAX_LEVEL_SIZE] but
	/// targeting TTL-expired files.
	ROUND_ROBIN_TTL,

	/// Internal: `DBImpl::ReFitLevel`, accounted for as a compaction for conflict-checking.
	REFIT_LEVEL,

	/// Compaction triggered by a high read frequency on SST files.
	READ_TRIGGERED;

	static CompactionReason fromValue(int value) {
		return switch (value) {
			case 0 -> UNKNOWN;
			case 1 -> LEVEL_L0_FILES_NUM;
			case 2 -> LEVEL_MAX_LEVEL_SIZE;
			case 3 -> UNIVERSAL_SIZE_AMPLIFICATION;
			case 4 -> UNIVERSAL_SIZE_RATIO;
			case 5 -> UNIVERSAL_SORTED_RUN_NUM;
			case 6 -> FIFO_MAX_SIZE;
			case 7 -> FIFO_REDUCE_NUM_FILES;
			case 8 -> FIFO_TTL;
			case 9 -> MANUAL_COMPACTION;
			case 10 -> FILES_MARKED_FOR_COMPACTION;
			case 11 -> BOTTOMMOST_FILES;
			case 12 -> TTL;
			case 13 -> FLUSH;
			case 14 -> EXTERNAL_SST_INGESTION;
			case 15 -> PERIODIC_COMPACTION;
			case 16 -> CHANGE_TEMPERATURE;
			case 17 -> FORCED_BLOB_GC;
			case 18 -> ROUND_ROBIN_TTL;
			case 19 -> REFIT_LEVEL;
			case 20 -> READ_TRIGGERED;
			default -> throw new IllegalArgumentException("Unknown compaction reason value: " + value);
		};
	}
}
