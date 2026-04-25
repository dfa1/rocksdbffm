package io.github.dfa1.rocksdbffm;

/// Ticker types for RocksDB statistics.
/// Corresponds to `enum Tickers` in `rocksdb/statistics.h`.
///
/// *NB*: beware this must be regenerated every time a new version of rocksdb is imported (!)
public enum TickerType {
	/// Number of block cache misses.
	BLOCK_CACHE_MISS(0),
	/// Number of block cache hits.
	BLOCK_CACHE_HIT(1),
	/// Number of blocks added to the block cache.
	BLOCK_CACHE_ADD(2),
	/// Number of failures when adding a block to the block cache.
	BLOCK_CACHE_ADD_FAILURES(3),
	/// Number of index block misses in the block cache.
	BLOCK_CACHE_INDEX_MISS(4),
	/// Number of index block hits in the block cache.
	BLOCK_CACHE_INDEX_HIT(5),
	/// Number of index blocks added to the block cache.
	BLOCK_CACHE_INDEX_ADD(6),
	/// Bytes of index blocks inserted into the block cache.
	BLOCK_CACHE_INDEX_BYTES_INSERT(7),
	/// Number of filter block misses in the block cache.
	BLOCK_CACHE_FILTER_MISS(8),
	/// Number of filter block hits in the block cache.
	BLOCK_CACHE_FILTER_HIT(9),
	/// Number of filter blocks added to the block cache.
	BLOCK_CACHE_FILTER_ADD(10),
	/// Bytes of filter blocks inserted into the block cache.
	BLOCK_CACHE_FILTER_BYTES_INSERT(11),
	/// Number of data block misses in the block cache.
	BLOCK_CACHE_DATA_MISS(12),
	/// Number of data block hits in the block cache.
	BLOCK_CACHE_DATA_HIT(13),
	/// Number of data blocks added to the block cache.
	BLOCK_CACHE_DATA_ADD(14),
	/// Bytes of data blocks inserted into the block cache.
	BLOCK_CACHE_DATA_BYTES_INSERT(15),
	/// Bytes read from the block cache.
	BLOCK_CACHE_BYTES_READ(16),
	/// Bytes written to the block cache.
	BLOCK_CACHE_BYTES_WRITE(17),
	/// Number of compression dictionary block misses in the block cache.
	BLOCK_CACHE_COMPRESSION_DICT_MISS(18),
	/// Number of compression dictionary block hits in the block cache.
	BLOCK_CACHE_COMPRESSION_DICT_HIT(19),
	/// Number of compression dictionary blocks added to the block cache.
	BLOCK_CACHE_COMPRESSION_DICT_ADD(20),
	/// Bytes of compression dictionary blocks inserted into the block cache.
	BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT(21),
	/// Number of redundant block additions to the block cache.
	BLOCK_CACHE_ADD_REDUNDANT(22),
	/// Number of redundant index block additions.
	BLOCK_CACHE_INDEX_ADD_REDUNDANT(23),
	/// Number of redundant filter block additions.
	BLOCK_CACHE_FILTER_ADD_REDUNDANT(24),
	/// Number of redundant data block additions.
	BLOCK_CACHE_DATA_ADD_REDUNDANT(25),
	/// Number of redundant compression dictionary block additions.
	BLOCK_CACHE_COMPRESSION_DICT_ADD_REDUNDANT(26),
	/// Number of hits served from the secondary cache.
	SECONDARY_CACHE_HITS(27),
	/// Number of filter block hits from the secondary cache.
	SECONDARY_CACHE_FILTER_HITS(28),
	/// Number of index block hits from the secondary cache.
	SECONDARY_CACHE_INDEX_HITS(29),
	/// Number of data block hits from the secondary cache.
	SECONDARY_CACHE_DATA_HITS(30),
	/// Number of dummy hits in the compressed secondary cache.
	COMPRESSED_SECONDARY_CACHE_DUMMY_HITS(31),
	/// Number of hits in the compressed secondary cache.
	COMPRESSED_SECONDARY_CACHE_HITS(32),
	/// Number of promotions from the compressed secondary cache.
	COMPRESSED_SECONDARY_CACHE_PROMOTIONS(33),
	/// Number of skipped promotions from the compressed secondary cache.
	COMPRESSED_SECONDARY_CACHE_PROMOTION_SKIPS(34),
	/// Number of times a Bloom filter was useful (avoided a read).
	BLOOM_FILTER_USEFUL(35),
	/// Number of full Bloom filter positive matches.
	BLOOM_FILTER_FULL_POSITIVE(36),
	/// Number of full Bloom filter true positive matches.
	BLOOM_FILTER_FULL_TRUE_POSITIVE(37),
	/// Number of prefix Bloom filter checks.
	BLOOM_FILTER_PREFIX_CHECKED(38),
	/// Number of prefix Bloom filter useful (avoided a read).
	BLOOM_FILTER_PREFIX_USEFUL(39),
	/// Number of prefix Bloom filter true positive matches.
	BLOOM_FILTER_PREFIX_TRUE_POSITIVE(40),
	/// Number of persistent cache hits.
	PERSISTENT_CACHE_HIT(41),
	/// Number of persistent cache misses.
	PERSISTENT_CACHE_MISS(42),
	/// Number of simulated block cache hits.
	SIM_BLOCK_CACHE_HIT(43),
	/// Number of simulated block cache misses.
	SIM_BLOCK_CACHE_MISS(44),
	/// Number of memtable hits.
	MEMTABLE_HIT(45),
	/// Number of memtable misses.
	MEMTABLE_MISS(46),
	/// Number of Get hits at level 0.
	GET_HIT_L0(47),
	/// Number of Get hits at level 1.
	GET_HIT_L1(48),
	/// Number of Get hits at level 2 and above.
	GET_HIT_L2_AND_UP(49),
	/// Keys dropped during compaction because a newer entry exists.
	COMPACTION_KEY_DROP_NEWER_ENTRY(50),
	/// Keys dropped during compaction because they are obsolete.
	COMPACTION_KEY_DROP_OBSOLETE(51),
	/// Keys dropped during compaction by range tombstones.
	COMPACTION_KEY_DROP_RANGE_DEL(52),
	/// Keys dropped during compaction by user compaction filter.
	COMPACTION_KEY_DROP_USER(53),
	/// Range deletions dropped during compaction as obsolete.
	COMPACTION_RANGE_DEL_DROP_OBSOLETE(54),
	/// Deletions optimized away during compaction.
	COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE(55),
	/// Number of compactions cancelled due to full output level.
	COMPACTION_CANCELLED(56),
	/// Number of compactions aborted.
	COMPACTION_ABORTED(57),
	/// Total number of keys written.
	NUMBER_KEYS_WRITTEN(58),
	/// Total number of keys read.
	NUMBER_KEYS_READ(59),
	/// Total number of keys updated (merge operations).
	NUMBER_KEYS_UPDATED(60),
	/// Total bytes written to the DB.
	BYTES_WRITTEN(61),
	/// Total bytes read from the DB.
	BYTES_READ(62),
	/// Total number of DB Seek iterator calls.
	NUMBER_DB_SEEK(63),
	/// Total number of DB Next iterator calls.
	NUMBER_DB_NEXT(64),
	/// Total number of DB Prev iterator calls.
	NUMBER_DB_PREV(65),
	/// Number of DB Seek calls that found a valid key.
	NUMBER_DB_SEEK_FOUND(66),
	/// Number of DB Next calls that found a valid key.
	NUMBER_DB_NEXT_FOUND(67),
	/// Number of DB Prev calls that found a valid key.
	NUMBER_DB_PREV_FOUND(68),
	/// Bytes read during iterator operations.
	ITER_BYTES_READ(69),
	/// Number of internal keys skipped during iteration.
	NUMBER_ITER_SKIP(70),
	/// Number of re-seeks performed during iteration.
	NUMBER_OF_RESEEKS_IN_ITERATION(71),
	/// Total number of iterators created.
	NO_ITERATOR_CREATED(72),
	/// Total number of iterators deleted.
	NO_ITERATOR_DELETED(73),
	/// Number of file opens.
	NO_FILE_OPENS(74),
	/// Number of file errors.
	NO_FILE_ERRORS(75),
	/// Total microseconds spent in write stalls.
	STALL_MICROS(76),
	/// Total microseconds spent waiting for the DB mutex.
	DB_MUTEX_WAIT_MICROS(77),
	/// Total number of MultiGet calls.
	NUMBER_MULTIGET_CALLS(78),
	/// Total number of keys read via MultiGet.
	NUMBER_MULTIGET_KEYS_READ(79),
	/// Total bytes read via MultiGet.
	NUMBER_MULTIGET_BYTES_READ(80),
	/// Total number of keys found via MultiGet.
	NUMBER_MULTIGET_KEYS_FOUND(81),
	/// Number of merge operation failures.
	NUMBER_MERGE_FAILURES(82),
	/// Number of GetUpdatesSince calls.
	GET_UPDATES_SINCE_CALLS(83),
	/// Number of WAL file syncs.
	WAL_FILE_SYNCED(84),
	/// Total bytes written to WAL files.
	WAL_FILE_BYTES(85),
	/// Number of writes done by the writing thread itself.
	WRITE_DONE_BY_SELF(86),
	/// Number of writes done by another thread in a group commit.
	WRITE_DONE_BY_OTHER(87),
	/// Number of writes that included a WAL write.
	WRITE_WITH_WAL(88),
	/// Bytes read during compaction.
	COMPACT_READ_BYTES(89),
	/// Bytes written during compaction.
	COMPACT_WRITE_BYTES(90),
	/// Bytes written during flush.
	FLUSH_WRITE_BYTES(91),
	/// Compaction bytes read for explicitly marked files.
	COMPACT_READ_BYTES_MARKED(92),
	/// Compaction bytes read for periodic compaction.
	COMPACT_READ_BYTES_PERIODIC(93),
	/// Compaction bytes read for TTL compaction.
	COMPACT_READ_BYTES_TTL(94),
	/// Compaction bytes written for explicitly marked files.
	COMPACT_WRITE_BYTES_MARKED(95),
	/// Compaction bytes written for periodic compaction.
	COMPACT_WRITE_BYTES_PERIODIC(96),
	/// Compaction bytes written for TTL compaction.
	COMPACT_WRITE_BYTES_TTL(97),
	/// Number of times table properties were loaded directly.
	NUMBER_DIRECT_LOAD_TABLE_PROPERTIES(98),
	/// Number of superversion acquisitions.
	NUMBER_SUPERVERSION_ACQUIRES(99),
	/// Number of superversion releases.
	NUMBER_SUPERVERSION_RELEASES(100),
	/// Number of superversion cleanups.
	NUMBER_SUPERVERSION_CLEANUPS(101),
	/// Number of blocks compressed.
	NUMBER_BLOCK_COMPRESSED(102),
	/// Number of blocks decompressed.
	NUMBER_BLOCK_DECOMPRESSED(103),
	/// Bytes before compression.
	BYTES_COMPRESSED_FROM(104),
	/// Bytes after compression.
	BYTES_COMPRESSED_TO(105),
	/// Bytes bypassed by compression (stored uncompressed).
	BYTES_COMPRESSION_BYPASSED(106),
	/// Bytes rejected by compression filter.
	BYTES_COMPRESSION_REJECTED(107),
	/// Number of blocks that bypassed compression.
	NUMBER_BLOCK_COMPRESSION_BYPASSED(108),
	/// Number of blocks rejected by compression filter.
	NUMBER_BLOCK_COMPRESSION_REJECTED(109),
	/// Bytes before decompression.
	BYTES_DECOMPRESSED_FROM(110),
	/// Bytes after decompression.
	BYTES_DECOMPRESSED_TO(111),
	/// Total time spent in merge operations, in microseconds.
	MERGE_OPERATION_TOTAL_TIME(112),
	/// Total time spent in filter operations, in microseconds.
	FILTER_OPERATION_TOTAL_TIME(113),
	/// Total CPU time spent in compaction, in microseconds.
	COMPACTION_CPU_TOTAL_TIME(114),
	/// Number of row cache hits.
	ROW_CACHE_HIT(115),
	/// Number of row cache misses.
	ROW_CACHE_MISS(116),
	/// Estimate of useful bytes read (read amplification numerator).
	READ_AMP_ESTIMATE_USEFUL_BYTES(117),
	/// Total bytes read including amplification (read amplification denominator).
	READ_AMP_TOTAL_READ_BYTES(118),
	/// Number of rate limiter drains.
	NUMBER_RATE_LIMITER_DRAINS(119),
	/// Number of blob DB Put operations.
	BLOB_DB_NUM_PUT(120),
	/// Number of blob DB Write operations.
	BLOB_DB_NUM_WRITE(121),
	/// Number of blob DB Get operations.
	BLOB_DB_NUM_GET(122),
	/// Number of blob DB MultiGet operations.
	BLOB_DB_NUM_MULTIGET(123),
	/// Number of blob DB Seek operations.
	BLOB_DB_NUM_SEEK(124),
	/// Number of blob DB Next operations.
	BLOB_DB_NUM_NEXT(125),
	/// Number of blob DB Prev operations.
	BLOB_DB_NUM_PREV(126),
	/// Number of keys written to blob DB.
	BLOB_DB_NUM_KEYS_WRITTEN(127),
	/// Number of keys read from blob DB.
	BLOB_DB_NUM_KEYS_READ(128),
	/// Bytes written to blob DB.
	BLOB_DB_BYTES_WRITTEN(129),
	/// Bytes read from blob DB.
	BLOB_DB_BYTES_READ(130),
	/// Deprecated: number of inlined blob DB writes.
	BLOB_DB_WRITE_INLINED_DEPRECATED(131),
	/// Deprecated: number of inlined TTL blob DB writes.
	BLOB_DB_WRITE_INLINED_TTL_DEPRECATED(132),
	/// Number of values written as blobs.
	BLOB_DB_WRITE_BLOB(133),
	/// Number of values written as TTL blobs.
	BLOB_DB_WRITE_BLOB_TTL(134),
	/// Bytes written to blob files.
	BLOB_DB_BLOB_FILE_BYTES_WRITTEN(135),
	/// Bytes read from blob files.
	BLOB_DB_BLOB_FILE_BYTES_READ(136),
	/// Number of blob file syncs.
	BLOB_DB_BLOB_FILE_SYNCED(137),
	/// Number of expired blob index entries.
	BLOB_DB_BLOB_INDEX_EXPIRED_COUNT(138),
	/// Size of expired blob index entries.
	BLOB_DB_BLOB_INDEX_EXPIRED_SIZE(139),
	/// Number of evicted blob index entries.
	BLOB_DB_BLOB_INDEX_EVICTED_COUNT(140),
	/// Size of evicted blob index entries.
	BLOB_DB_BLOB_INDEX_EVICTED_SIZE(141),
	/// Number of blob files considered for GC.
	BLOB_DB_GC_NUM_FILES(142),
	/// Number of new blob files created during GC.
	BLOB_DB_GC_NUM_NEW_FILES(143),
	/// Number of blob GC failures.
	BLOB_DB_GC_FAILURES(144),
	/// Number of keys relocated during blob GC.
	BLOB_DB_GC_NUM_KEYS_RELOCATED(145),
	/// Bytes relocated during blob GC.
	BLOB_DB_GC_BYTES_RELOCATED(146),
	/// Number of files evicted by FIFO compaction.
	BLOB_DB_FIFO_NUM_FILES_EVICTED(147),
	/// Number of keys evicted by FIFO compaction.
	BLOB_DB_FIFO_NUM_KEYS_EVICTED(148),
	/// Bytes evicted by FIFO compaction.
	BLOB_DB_FIFO_BYTES_EVICTED(149),
	/// Number of blob cache misses.
	BLOB_DB_CACHE_MISS(150),
	/// Number of blob cache hits.
	BLOB_DB_CACHE_HIT(151),
	/// Number of blob cache adds.
	BLOB_DB_CACHE_ADD(152),
	/// Number of blob cache add failures.
	BLOB_DB_CACHE_ADD_FAILURES(153),
	/// Bytes read from the blob cache.
	BLOB_DB_CACHE_BYTES_READ(154),
	/// Bytes written to the blob cache.
	BLOB_DB_CACHE_BYTES_WRITE(155),
	/// Overhead from transaction prepare mutex.
	TXN_PREPARE_MUTEX_OVERHEAD(156),
	/// Overhead from transaction old commit map mutex.
	TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD(157),
	/// Overhead from duplicate key checks in transactions.
	TXN_DUPLICATE_KEY_OVERHEAD(158),
	/// Overhead from transaction snapshot mutex.
	TXN_SNAPSHOT_MUTEX_OVERHEAD(159),
	/// Number of transaction Get try-again retries.
	TXN_GET_TRY_AGAIN(160),
	/// Number of files marked as trash for background deletion.
	FILES_MARKED_TRASH(161),
	/// Number of files deleted from the trash queue.
	FILES_DELETED_FROM_TRASH_QUEUE(162),
	/// Number of files deleted immediately (not via trash queue).
	FILES_DELETED_IMMEDIATELY(163),
	/// Number of background errors handled by the error handler.
	ERROR_HANDLER_BG_ERROR_COUNT(164),
	/// Number of background I/O errors handled by the error handler.
	ERROR_HANDLER_BG_IO_ERROR_COUNT(165),
	/// Number of retryable background I/O errors.
	ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT(166),
	/// Number of automatic error resume attempts.
	ERROR_HANDLER_AUTORESUME_COUNT(167),
	/// Total number of auto-resume retry attempts.
	ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT(168),
	/// Number of successful auto-resume operations.
	ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT(169),
	/// Bytes of useful payload in memtable at flush.
	MEMTABLE_PAYLOAD_BYTES_AT_FLUSH(170),
	/// Bytes of garbage in memtable at flush.
	MEMTABLE_GARBAGE_BYTES_AT_FLUSH(171),
	/// Bytes read during checksum verification.
	VERIFY_CHECKSUM_READ_BYTES(172),
	/// Bytes read during backup.
	BACKUP_READ_BYTES(173),
	/// Bytes written during backup.
	BACKUP_WRITE_BYTES(174),
	/// Bytes read during remote compaction.
	REMOTE_COMPACT_READ_BYTES(175),
	/// Bytes written during remote compaction.
	REMOTE_COMPACT_WRITE_BYTES(176),
	/// Bytes resumed from previous remote compaction.
	REMOTE_COMPACT_RESUMED_BYTES(177),
	/// Bytes read from hot-temperature files.
	HOT_FILE_READ_BYTES(178),
	/// Bytes read from warm-temperature files.
	WARM_FILE_READ_BYTES(179),
	/// Bytes read from cool-temperature files.
	COOL_FILE_READ_BYTES(180),
	/// Bytes read from cold-temperature files.
	COLD_FILE_READ_BYTES(181),
	/// Bytes read from ice-temperature files.
	ICE_FILE_READ_BYTES(182),
	/// Number of reads from hot-temperature files.
	HOT_FILE_READ_COUNT(183),
	/// Number of reads from warm-temperature files.
	WARM_FILE_READ_COUNT(184),
	/// Number of reads from cool-temperature files.
	COOL_FILE_READ_COUNT(185),
	/// Number of reads from cold-temperature files.
	COLD_FILE_READ_COUNT(186),
	/// Number of reads from ice-temperature files.
	ICE_FILE_READ_COUNT(187),
	/// Bytes read from the last LSM level.
	LAST_LEVEL_READ_BYTES(188),
	/// Number of reads from the last LSM level.
	LAST_LEVEL_READ_COUNT(189),
	/// Bytes read from non-last LSM levels.
	NON_LAST_LEVEL_READ_BYTES(190),
	/// Number of reads from non-last LSM levels.
	NON_LAST_LEVEL_READ_COUNT(191),
	/// Number of seeks on the last level filtered by Bloom/index.
	LAST_LEVEL_SEEK_FILTERED(192),
	/// Number of seeks on the last level that matched after Bloom/index filter.
	LAST_LEVEL_SEEK_FILTER_MATCH(193),
	/// Number of seeks on the last level that read data.
	LAST_LEVEL_SEEK_DATA(194),
	/// Seeks on the last level reading useful data, no filter.
	LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER(195),
	/// Seeks on the last level reading useful data with a filter match.
	LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH(196),
	/// Number of seeks on non-last levels filtered by Bloom/index.
	NON_LAST_LEVEL_SEEK_FILTERED(197),
	/// Number of seeks on non-last levels matching after Bloom/index filter.
	NON_LAST_LEVEL_SEEK_FILTER_MATCH(198),
	/// Number of seeks on non-last levels reading data.
	NON_LAST_LEVEL_SEEK_DATA(199),
	/// Seeks on non-last levels reading useful data, no filter.
	NON_LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER(200),
	/// Seeks on non-last levels reading useful data with filter match.
	NON_LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH(201),
	/// Number of block checksum computations.
	BLOCK_CHECKSUM_COMPUTE_COUNT(202),
	/// Number of block checksum mismatches detected.
	BLOCK_CHECKSUM_MISMATCH_COUNT(203),
	/// Number of MultiGet coroutines invoked.
	MULTIGET_COROUTINE_COUNT(204),
	/// Microseconds spent in async reads.
	READ_ASYNC_MICROS(205),
	/// Number of errors during async reads.
	ASYNC_READ_ERROR_COUNT(206),
	/// Number of table open prefetch tail misses.
	TABLE_OPEN_PREFETCH_TAIL_MISS(207),
	/// Number of table open prefetch tail hits.
	TABLE_OPEN_PREFETCH_TAIL_HIT(208),
	/// Number of tables checked by the timestamp filter.
	TIMESTAMP_FILTER_TABLE_CHECKED(209),
	/// Number of tables filtered out by the timestamp filter.
	TIMESTAMP_FILTER_TABLE_FILTERED(210),
	/// Number of readahead trims.
	READAHEAD_TRIMMED(211),
	/// Number of FIFO compactions triggered by max size.
	FIFO_MAX_SIZE_COMPACTIONS(212),
	/// Number of FIFO compactions triggered by TTL.
	FIFO_TTL_COMPACTIONS(213),
	/// Number of FIFO compactions triggered by temperature changes.
	FIFO_CHANGE_TEMPERATURE_COMPACTIONS(214),
	/// Total bytes prefetched.
	PREFETCH_BYTES(215),
	/// Useful bytes from prefetching (actually consumed).
	PREFETCH_BYTES_USEFUL(216),
	/// Number of prefetch hits.
	PREFETCH_HITS(217),
	/// Number of SST file footer corruption events.
	SST_FOOTER_CORRUPTION_COUNT(218),
	/// Number of file read corruption retry attempts.
	FILE_READ_CORRUPTION_RETRY_COUNT(219),
	/// Number of successful file read corruption retries.
	FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT(220),
	/// Number of WBWI ingest operations.
	NUMBER_WBWI_INGEST(221),
	/// Number of failures loading user-defined SST index.
	SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT(222),
	/// Number of multiscan prepare calls.
	MULTISCAN_PREPARE_CALLS(223),
	/// Number of multiscan prepare errors.
	MULTISCAN_PREPARE_ERRORS(224),
	/// Number of blocks prefetched during multiscan.
	MULTISCAN_BLOCKS_PREFETCHED(225),
	/// Number of blocks served from cache during multiscan.
	MULTISCAN_BLOCKS_FROM_CACHE(226),
	/// Bytes prefetched during multiscan.
	MULTISCAN_PREFETCH_BYTES(227),
	/// Prefetched blocks wasted during multiscan.
	MULTISCAN_PREFETCH_BLOCKS_WASTED(228),
	/// Number of I/O requests during multiscan.
	MULTISCAN_IO_REQUESTS(229),
	/// Number of coalesced non-adjacent I/O requests during multiscan.
	MULTISCAN_IO_COALESCED_NONADJACENT(230),
	/// Number of seek errors during multiscan.
	MULTISCAN_SEEK_ERRORS(231),
	/// Bytes granted for prefetch memory.
	PREFETCH_MEMORY_BYTES_GRANTED(232),
	/// Bytes released from prefetch memory.
	PREFETCH_MEMORY_BYTES_RELEASED(233),
	/// Number of prefetch memory requests blocked.
	PREFETCH_MEMORY_REQUESTS_BLOCKED(234),
	;

	private final int value;

	TickerType(int value) {
		this.value = value;
	}

	// don't expose this
	int getValue() {
		return value;
	}
}
