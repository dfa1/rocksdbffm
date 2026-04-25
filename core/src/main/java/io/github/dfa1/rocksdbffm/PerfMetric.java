package io.github.dfa1.rocksdbffm;

/// Identifies a single counter or timer inside a [PerfContext].
///
/// Pass to [PerfContext#metric(PerfMetric)] to read the accumulated value
/// for the current thread since the last [PerfContext#reset()].
public enum PerfMetric {

	/// Number of user key comparisons performed.
	USER_KEY_COMPARISON_COUNT(0),
	/// Number of block cache hits.
	BLOCK_CACHE_HIT_COUNT(1),
	/// Number of blocks read from storage.
	BLOCK_READ_COUNT(2),
	/// Total bytes read from blocks.
	BLOCK_READ_BYTE(3),
	/// Time spent reading blocks, in nanoseconds.
	BLOCK_READ_TIME(4),
	/// Time spent computing block checksums, in nanoseconds.
	BLOCK_CHECKSUM_TIME(5),
	/// Time spent decompressing blocks, in nanoseconds.
	BLOCK_DECOMPRESS_TIME(6),
	/// Bytes read during Get operations.
	GET_READ_BYTES(7),
	/// Bytes read during MultiGet operations.
	MULTIGET_READ_BYTES(8),
	/// Bytes read during iterator operations.
	ITER_READ_BYTES(9),
	/// Number of internal keys skipped during iteration.
	INTERNAL_KEY_SKIPPED_COUNT(10),
	/// Number of internal delete markers skipped during iteration.
	INTERNAL_DELETE_SKIPPED_COUNT(11),
	/// Number of recently obsolete entries skipped during iteration.
	INTERNAL_RECENT_SKIPPED_COUNT(12),
	/// Number of merge operations applied.
	INTERNAL_MERGE_COUNT(13),
	/// Time spent acquiring a snapshot, in nanoseconds.
	GET_SNAPSHOT_TIME(14),
	/// Time spent reading from memtables, in nanoseconds.
	GET_FROM_MEMTABLE_TIME(15),
	/// Number of memtable entries examined during Get.
	GET_FROM_MEMTABLE_COUNT(16),
	/// Time spent in Get post-processing, in nanoseconds.
	GET_POST_PROCESS_TIME(17),
	/// Time spent reading from SST files, in nanoseconds.
	GET_FROM_OUTPUT_FILES_TIME(18),
	/// Time spent seeking on memtables, in nanoseconds.
	SEEK_ON_MEMTABLE_TIME(19),
	/// Number of memtable seeks.
	SEEK_ON_MEMTABLE_COUNT(20),
	/// Number of Next calls on memtables.
	NEXT_ON_MEMTABLE_COUNT(21),
	/// Number of Prev calls on memtables.
	PREV_ON_MEMTABLE_COUNT(22),
	/// Time spent on child iterator seeks, in nanoseconds.
	SEEK_CHILD_SEEK_TIME(23),
	/// Number of child iterator seek calls.
	SEEK_CHILD_SEEK_COUNT(24),
	/// Time spent maintaining the min heap during seek, in nanoseconds.
	SEEK_MIN_HEAP_TIME(25),
	/// Time spent maintaining the max heap during seek, in nanoseconds.
	SEEK_MAX_HEAP_TIME(26),
	/// Time spent on internal seeks, in nanoseconds.
	SEEK_INTERNAL_SEEK_TIME(27),
	/// Time spent finding the next user entry, in nanoseconds.
	FIND_NEXT_USER_ENTRY_TIME(28),
	/// Time spent writing to the WAL, in nanoseconds.
	WRITE_WAL_TIME(29),
	/// Time spent writing to memtables, in nanoseconds.
	WRITE_MEMTABLE_TIME(30),
	/// Time a write was delayed due to write stalls, in nanoseconds.
	WRITE_DELAY_TIME(31),
	/// Time spent in write pre/post processing, in nanoseconds.
	WRITE_PRE_AND_POST_PROCESS_TIME(32),
	/// Time spent waiting for the DB mutex, in nanoseconds.
	DB_MUTEX_LOCK_NANOS(33),
	/// Time spent waiting on a condition variable, in nanoseconds.
	DB_CONDITION_WAIT_NANOS(34),
	/// Time spent in merge operators, in nanoseconds.
	MERGE_OPERATOR_TIME_NANOS(35),
	/// Time spent reading index blocks, in nanoseconds.
	READ_INDEX_BLOCK_NANOS(36),
	/// Time spent reading filter blocks, in nanoseconds.
	READ_FILTER_BLOCK_NANOS(37),
	/// Time spent creating table block iterators, in nanoseconds.
	NEW_TABLE_BLOCK_ITER_NANOS(38),
	/// Time spent creating table iterators, in nanoseconds.
	NEW_TABLE_ITERATOR_NANOS(39),
	/// Time spent seeking within a block, in nanoseconds.
	BLOCK_SEEK_NANOS(40),
	/// Time spent finding the SST table file, in nanoseconds.
	FIND_TABLE_NANOS(41),
	/// Number of bloom filter hits on memtable.
	BLOOM_MEMTABLE_HIT_COUNT(42),
	/// Number of bloom filter misses on memtable.
	BLOOM_MEMTABLE_MISS_COUNT(43),
	/// Number of bloom filter hits on SST files.
	BLOOM_SST_HIT_COUNT(44),
	/// Number of bloom filter misses on SST files.
	BLOOM_SST_MISS_COUNT(45),
	/// Time spent waiting for key locks, in nanoseconds.
	KEY_LOCK_WAIT_TIME(46),
	/// Number of times a key lock was waited on.
	KEY_LOCK_WAIT_COUNT(47),
	/// Time spent opening sequential files in the env, in nanoseconds.
	ENV_NEW_SEQUENTIAL_FILE_NANOS(48),
	/// Time spent opening random-access files in the env, in nanoseconds.
	ENV_NEW_RANDOM_ACCESS_FILE_NANOS(49),
	/// Time spent opening writable files in the env, in nanoseconds.
	ENV_NEW_WRITABLE_FILE_NANOS(50),
	/// Time spent reusing writable files in the env, in nanoseconds.
	ENV_REUSE_WRITABLE_FILE_NANOS(51),
	/// Time spent opening random read-write files in the env, in nanoseconds.
	ENV_NEW_RANDOM_RW_FILE_NANOS(52),
	/// Time spent opening directories in the env, in nanoseconds.
	ENV_NEW_DIRECTORY_NANOS(53),
	/// Time spent in file-exists checks in the env, in nanoseconds.
	ENV_FILE_EXISTS_NANOS(54),
	/// Time spent listing directory children in the env, in nanoseconds.
	ENV_GET_CHILDREN_NANOS(55),
	/// Time spent getting file attributes for directory children, in nanoseconds.
	ENV_GET_CHILDREN_FILE_ATTRIBUTES_NANOS(56),
	/// Time spent deleting files in the env, in nanoseconds.
	ENV_DELETE_FILE_NANOS(57),
	/// Time spent creating directories in the env, in nanoseconds.
	ENV_CREATE_DIR_NANOS(58),
	/// Time spent in create-directory-if-missing calls in the env, in nanoseconds.
	ENV_CREATE_DIR_IF_MISSING_NANOS(59),
	/// Time spent deleting directories in the env, in nanoseconds.
	ENV_DELETE_DIR_NANOS(60),
	/// Time spent querying file sizes in the env, in nanoseconds.
	ENV_GET_FILE_SIZE_NANOS(61),
	/// Time spent querying file modification times in the env, in nanoseconds.
	ENV_GET_FILE_MODIFICATION_TIME_NANOS(62),
	/// Time spent renaming files in the env, in nanoseconds.
	ENV_RENAME_FILE_NANOS(63),
	/// Time spent creating hard links in the env, in nanoseconds.
	ENV_LINK_FILE_NANOS(64),
	/// Time spent locking files in the env, in nanoseconds.
	ENV_LOCK_FILE_NANOS(65),
	/// Time spent unlocking files in the env, in nanoseconds.
	ENV_UNLOCK_FILE_NANOS(66),
	/// Time spent creating loggers in the env, in nanoseconds.
	ENV_NEW_LOGGER_NANOS(67),
	/// Number of asynchronous seek operations initiated.
	NUMBER_ASYNC_SEEK(68),
	/// Number of blob cache hits.
	BLOB_CACHE_HIT_COUNT(69),
	/// Number of blob reads.
	BLOB_READ_COUNT(70),
	/// Total bytes read from blobs.
	BLOB_READ_BYTE(71),
	/// Time spent reading blobs, in nanoseconds.
	BLOB_READ_TIME(72),
	/// Time spent computing blob checksums, in nanoseconds.
	BLOB_CHECKSUM_TIME(73),
	/// Time spent decompressing blobs, in nanoseconds.
	BLOB_DECOMPRESS_TIME(74),
	/// Number of re-seeks caused by range deletions during iteration.
	INTERNAL_RANGE_DEL_RESEEK_COUNT(75),
	/// CPU time spent reading blocks, in nanoseconds.
	BLOCK_READ_CPU_TIME(76),
	/// Number of merge operations applied during point lookups.
	INTERNAL_MERGE_POINT_LOOKUP_COUNT(77);

	final int value;

	PerfMetric(int value) {
		this.value = value;
	}
}
