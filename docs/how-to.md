# How-to guides

Task-oriented recipes. Each section solves one concrete goal and assumes you already have a working
project — if you don't, start with the [tutorial](tutorial.md).

For class-by-class API details see [reference.md](reference.md); for the reasoning behind the
patterns used here see [explanation.md](explanation.md).

Every snippet omits imports; all types live in `io.github.dfa1.rocksdbffm`.

- [Pick an access tier](#pick-an-access-tier)
- [Open a database read-only](#open-a-database-read-only)
- [Use column families](#use-column-families)
- [Read a consistent point-in-time view](#read-a-consistent-point-in-time-view)
- [Scan a key range](#scan-a-key-range)
- [Delete a range of keys](#delete-a-range-of-keys)
- [Merge values without a read-modify-write](#merge-values-without-a-read-modify-write)
- [Run a pessimistic transaction](#run-a-pessimistic-transaction)
- [Run an optimistic transaction](#run-an-optimistic-transaction)
- [Expire keys automatically](#expire-keys-automatically)
- [Take a checkpoint](#take-a-checkpoint)
- [Back up and restore](#back-up-and-restore)
- [Bulk-load data with SST files](#bulk-load-data-with-sst-files)
- [Tail the write-ahead log](#tail-the-write-ahead-log)
- [Serve reads from a secondary instance](#serve-reads-from-a-secondary-instance)
- [Store large values in blob files](#store-large-values-in-blob-files)
- [Tune the block cache and bloom filters](#tune-the-block-cache-and-bloom-filters)
- [Throttle background I/O](#throttle-background-io)
- [Cap disk usage](#cap-disk-usage)
- [Inspect properties and statistics](#inspect-properties-and-statistics)
- [Profile a single operation](#profile-a-single-operation)
- [Route RocksDB logs into your logger](#route-rocksdb-logs-into-your-logger)
- [Observe flushes and compactions](#observe-flushes-and-compactions)
- [Compact manually](#compact-manually)
- [Load a custom native library](#load-a-custom-native-library)
- [Build the native library from source](#build-the-native-library-from-source)
- [Run the benchmarks](#run-the-benchmarks)

---

## Pick an access tier

Every read and write exists in three tiers, differing only in how key and value bytes cross the
Java/native boundary. Reads have a fourth, zero-copy option on top: instead of copying the value
into a destination you provide, `get(key, Mapper)` hands your callback a view directly into
RocksDB's own pinned memory.

```java
// byte[] — convenience. Allocates and copies on every call.
db.put("k".getBytes(), "v".getBytes());
byte[] value = db.get("k".getBytes());

// ByteBuffer — for NIO-based code. Use direct buffers; heap buffers force a copy.
ByteBuffer key = ByteBuffer.allocateDirect(64);
ByteBuffer dst = ByteBuffer.allocateDirect(1024);
switch (db.get(key, dst)) {
	case CopyResult.Copied() -> dst.flip();
	case CopyResult.NotEnoughCapacity(long required) -> retryWith(required);
	case CopyResult.NotFound() -> handleMiss();
}

// MemorySegment — you own the arena, so you own the lifetime. Still one copy,
// into your own destination segment.
try (Arena arena = Arena.ofConfined()) {
	MemorySegment k = arena.allocateFrom("k");
	MemorySegment v = arena.allocate(1024);
	CopyResult result = db.get(k, v);
}

// Mapper — fastest: no destination buffer at all. fn gets a read-only view
// straight into RocksDB's own pinned memory; nothing is copied.
try (Arena arena = Arena.ofConfined()) {
	MemorySegment k = arena.allocateFrom("k");
	Optional<Integer> len = db.get(k, value -> (int) value.byteSize());
}
```

The `byte[]` `get` returns `null` when the key is absent. The buffer/segment tiers return a
[`CopyResult`](reference.md#domain-types) instead, which distinguishes "missing" from "present but
your destination is too small" — a distinction the old `int` return could not make. `get(key, Mapper)`
returns an `Optional<R>` instead — there's no destination to be too small for, so the only two
outcomes are "present, mapped to a result" and "absent." See
[explanation.md#three-access-tiers](explanation.md#three-access-tiers) for why `byte[]`/`ByteBuffer`/
`MemorySegment` exist as three separate tiers, and for the zero-copy `Mapper` path layered on top of
the `MemorySegment` tier.

The `Mapper` view is only valid for the duration of the callback — it's bound to an arena that
closes the moment `fn` returns, so copy anything you need to keep (`value.toArray(JAVA_BYTE)`,
a parsed primitive, …) before returning. Retaining the view itself throws `IllegalStateException`.

## Open a database read-only

`ReadOnlyDB` has no `put`, `delete`, or `write` method at all, so a write attempt fails to compile
rather than at runtime:

```java
try (var db = RocksDB.openReadOnly(dbPath)) {
	byte[] value = db.get("k".getBytes());
}
```

Pass `errorIfWalFileExists = true` to refuse opening when the primary left an unflushed WAL behind:

```java
try (var options = Options.newOptions();
     var db = RocksDB.openReadOnly(options, dbPath, true)) {
	// ...
}
```

## Use column families

Create a column family on an open database:

```java
try (var db = RocksDB.openReadWrite(dbPath);
     var accounts = db.createColumnFamily(ColumnFamilyDescriptor.of("accounts"))) {
	db.put(accounts, "alice".getBytes(), "100".getBytes());
	byte[] balance = db.get(accounts, "alice".getBytes());
}
```

On reopen you must list **every** column family that exists in the database, including `default`,
or the open fails. Handles come back in the same order as the descriptors:

```java
List<ColumnFamilyHandle> handles = new ArrayList<>();
try (var options = Options.newOptions();
     var db = RocksDB.openReadWrite(options, dbPath,
		     List.of(ColumnFamilyDescriptor.of("default"),
				     ColumnFamilyDescriptor.of("accounts")),
		     handles)) {
	var accounts = handles.get(1);
	db.put(accounts, "bob".getBytes(), "200".getBytes());
} finally {
	handles.forEach(ColumnFamilyHandle::close);
}
```

Not sure what exists on disk? Ask first:

```java
try (var options = Options.newOptions()) {
	List<byte[]> names = RocksDB.listColumnFamilies(options, dbPath);
}
```

Every DB type has the same column-family overloads — see
[reference.md#column-families](reference.md#column-families).

## Read a consistent point-in-time view

A snapshot pins a sequence number; reads through it ignore everything written afterwards.

```java
try (var db = RocksDB.openReadWrite(dbPath)) {
	db.put("k".getBytes(), "before".getBytes());

	try (var snapshot = db.getSnapshot();
	     var readOptions = ReadOptions.newReadOptions().setSnapshot(snapshot)) {
		db.put("k".getBytes(), "after".getBytes());

		byte[] pinned = db.get(readOptions, "k".getBytes());   // "before"
		try (var it = db.newIterator(readOptions)) {           // also sees "before"
			for (it.seekToFirst(); it.isValid(); it.next()) { /* ... */ }
		}
	}
}
```

Close snapshots promptly: an open snapshot blocks compaction from dropping the versions it pins.
`snapshot.sequenceNumber()` returns a [`SequenceNumber`](reference.md#domain-types), which is also
what the WAL iterator consumes.

## Scan a key range

Seek to the start and stop when you pass the end yourself — there is no upper-bound option:

```java
byte[] end = "user:5".getBytes();
try (var it = db.newIterator()) {
	for (it.seek("user:1".getBytes()); it.isValid(); it.next()) {
		if (Arrays.compareUnsigned(it.key(), end) >= 0) {
			break;
		}
		process(it.key(), it.value());
	}
	it.checkError();
}
```

To walk backwards, use `seekForPrev` plus `prev`, or `seekToLast`:

```java
for (it.seekForPrev("user:9".getBytes()); it.isValid(); it.prev()) { /* ... */ }
```

Comparison must be **unsigned** — RocksDB orders keys as raw bytes, and Java's signed `byte`
comparison would place `0x80…0xFF` before `0x00`.

For the zero-copy variants (`key(Mapper)`, `value(Mapper)`) mind the lifetime rule: the view passed
to the callback is only valid for the duration of that call.

## Delete a range of keys

A range tombstone is one operation regardless of how many keys it covers — far cheaper than deleting
them individually. The end key is **exclusive**:

```java
db.deleteRange("user:1".getBytes(), "user:5".getBytes());   // deletes user:1 … user:4
```

It also works inside a batch and per column family:

```java
try (var batch = WriteBatch.create()) {
	batch.deleteRange("user:1".getBytes(), "user:5".getBytes());
	batch.put("user:9".getBytes(), "zoe".getBytes());
	db.write(batch);
}
```

## Merge values without a read-modify-write

`merge(key, operand)` queues an operand for `key` instead of overwriting it. RocksDB folds the
base value and all queued operands together — lazily, whenever the merged value is actually
needed (`get()`, flush, compaction) — using a `MergeOperator` you attach via
`Options.setMergeOperator`. Without one attached, every `merge()` call fails with
`RocksDBException`.

For counters, use the built-in operator: it sums 8-byte little-endian `uint64` operands, entirely
on the native side.

```java
try (var opts = Options.newOptions().setCreateIfMissing(true)
            .setMergeOperator(MergeOperator.uint64Add());
     var db = RocksDB.openReadWrite(opts, dbPath)) {

	db.merge("page-views".getBytes(), encodeUint64(1));
	db.merge("page-views".getBytes(), encodeUint64(1));
	db.merge("page-views".getBytes(), encodeUint64(3));

	long total = decodeUint64(db.get("page-views".getBytes()));   // 5, no read-modify-write
}
```

```java
static byte[] encodeUint64(long value) {
	return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
}

static long decodeUint64(byte[] bytes) {
	return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
}
```

For anything else, `MergeOperator.custom(name, fn)` wires a Java `FullMergeFn` through RocksDB's
callback-based merge operator. `fn` may run on RocksDB's own background threads (flush,
compaction), so it must be thread-safe and must not throw:

```java
MergeOperator.FullMergeFn keepMax = (key, existingValue, operands) -> {
	byte[] max = existingValue;
	for (byte[] operand : operands) {
		if (max == null || Arrays.compare(operand, max) > 0) {
			max = operand;
		}
	}
	return max;
};

try (var opts = Options.newOptions().setCreateIfMissing(true)
            .setMergeOperator(MergeOperator.custom("string-max", keepMax));
     var db = RocksDB.openReadWrite(opts, dbPath)) {

	db.merge("high-score".getBytes(), "17".getBytes());
	db.merge("high-score".getBytes(), "42".getBytes());
	db.merge("high-score".getBytes(), "9".getBytes());

	byte[] best = db.get("high-score".getBytes());   // "42"
}
```

A `MergeOperator` is attached once per column family at `Options`-configuration time — every
`merge()` on that column family shares the same interpretation of what merging means, so keep
different kinds of merge semantics (a counter vs. a max-tracker) in separate column families.

## Run a pessimistic transaction

`TransactionDB` locks keys as you touch them, so conflicts surface at write time.

```java
try (var options = Options.newOptions().setCreateIfMissing(true);
     var txnDbOptions = TransactionDBOptions.newTransactionDBOptions();
     var db = RocksDB.openTransaction(options, txnDbOptions, dbPath);
     var writeOptions = WriteOptions.newWriteOptions();
     var txn = db.beginTransaction(writeOptions)) {

	txn.put("a".getBytes(), "1".getBytes());
	txn.put("b".getBytes(), "2".getBytes());
	txn.commit();     // or txn.rollback()
}
```

Uncommitted writes are invisible to readers outside the transaction. To read a key *and* lock it for
the rest of the transaction, use `getForUpdate` — `exclusive = true` takes a write lock:

```java
try (var readOptions = ReadOptions.newReadOptions()) {
	byte[] current = txn.getForUpdate(readOptions, "counter".getBytes(), true);
	txn.put("counter".getBytes(), increment(current));
	txn.commit();
}
```

Savepoints roll back part of a transaction without abandoning all of it:

```java
txn.put("a".getBytes(), "1".getBytes());
txn.setSavePoint();
txn.put("b".getBytes(), "2".getBytes());
txn.rollbackToSavePoint();   // "b" is gone, "a" survives
txn.commit();
```

## Run an optimistic transaction

`OptimisticTransactionDB` takes no locks and detects conflicts at `commit()`, which then throws.
Prefer it when contention is rare.

```java
try (var options = Options.newOptions().setCreateIfMissing(true);
     var db = RocksDB.openOptimistic(options, dbPath);
     var writeOptions = WriteOptions.newWriteOptions();
     var txn = db.beginTransaction(writeOptions)) {

	txn.put("k".getBytes(), "v".getBytes());
	try {
		txn.commit();
	} catch (RocksDBException e) {
		// another writer touched the same key — retry the whole transaction
	}
}
```

## Expire keys automatically

`TtlDB` stamps each value with a write timestamp and drops expired entries during compaction.

```java
try (var db = RocksDB.openTtl(dbPath, Duration.ofHours(24))) {
	db.put("session:1".getBytes(), "...".getBytes());
}
```

Expiry is **lazy**: an expired key can still be returned until a compaction covering its range runs.
Force one with `db.compactRange()` if a test needs determinism. `Duration.ZERO` disables expiry.

## Take a checkpoint

A checkpoint is a point-in-time directory of hard links — near-instant and cheap on the same
filesystem.

```java
try (var db = RocksDB.openReadWrite(dbPath);
     var checkpoint = Checkpoint.newCheckpoint(db)) {
	checkpoint.exportTo(checkpointDir);   // directory must not already exist
}

try (var copy = RocksDB.openReadOnly(checkpointDir)) {
	byte[] value = copy.get("k".getBytes());
}
```

The single-argument `exportTo` always flushes the WAL first; `exportTo(dir, MemorySize)` flushes only
when the WAL exceeds the given size (pass `MemorySize.ofBytes(Long.MAX_VALUE)` to never flush).
`Checkpoint.newCheckpoint` has an overload for each DB type that supports it.

## Back up and restore

Backups are incremental across calls: unchanged SST files are shared, not recopied.

```java
try (var options = Options.newOptions().setCreateIfMissing(true);
     var db = RocksDB.openReadWrite(options, dbPath);
     var engine = BackupEngine.open(options, backupDir)) {

	db.put("k".getBytes(), "v".getBytes());
	engine.createNewBackup(db, true);      // true = flush the memtable first

	for (BackupInfo info : engine.getBackupInfo()) {
		System.out.println(info.backupId() + " " + info.size() + " " + info.numberOfFiles());
	}

	engine.purgeOldBackups(3);             // keep the 3 newest
	engine.verifyBackup(BackupId.of(1));   // throws if corrupt
}
```

Restoring writes into an empty directory:

```java
try (var options = Options.newOptions();
     var engine = BackupEngine.open(options, backupDir);
     var restoreOptions = RestoreOptions.create()) {
	engine.restoreDbFromLatestBackup(restoreDir, restoreOptions);
	// or engine.restoreDbFromBackup(BackupId.of(2), restoreDir, restoreOptions);
}
```

Pass `flushBeforeBackup = false` only if you also restore the WAL directory; otherwise WAL-only
writes will be missing from the restored database. For rate limits, shared-file naming, and the
other knobs use the `BackupEngineOptions` overload of `BackupEngine.open` — see
[reference.md#backup-checkpoint-ingest](reference.md#backup-checkpoint-ingest).

## Bulk-load data with SST files

Writing an SST file directly and ingesting it bypasses the memtable and WAL entirely. Keys must be
added in **sorted order**.

```java
try (var options = Options.newOptions().setCreateIfMissing(true);
     var writer = SstFileWriter.newSstFileWriter(options)) {
	writer.open(sstPath);
	writer.put("aaa".getBytes(), "v1".getBytes());
	writer.put("bbb".getBytes(), "v2".getBytes());
	writer.finish();
}

try (var db = RocksDB.openReadWrite(dbPath)) {
	db.ingestExternalFile(sstPath);
	// or db.ingestExternalFile(List.of(sst1, sst2));
}
```

`IngestExternalFileOptions.newIngestExternalFileOptions().setMoveFiles(true)` moves instead of
copying when the files are on the same filesystem.

## Tail the write-ahead log

Useful for change-data-capture, replication, and auditing: read every batch written after a known
sequence number.

```java
try (var db = RocksDB.openReadWrite(dbPath)) {
	SequenceNumber from = db.getLatestSequenceNumber();

	db.put("a".getBytes(), "1".getBytes());
	db.put("b".getBytes(), "2".getBytes());

	try (WalIterator it = db.getUpdatesSince(from)) {
		for (; it.isValid(); it.next()) {
			try (WalBatchResult batch = it.getBatch()) {
				System.out.println(batch.sequenceNumber() + ": " + batch.writeBatch().count());
			}
		}
		it.checkStatus();
	}
}
```

Each `WalBatchResult` owns a `WriteBatch` and must be closed. Old WAL files are recycled, so a
sequence number that has already been compacted away is no longer reachable — checkpoint your
position often.

## Serve reads from a secondary instance

A secondary opens the *same* directory as a running primary, read-only, and catches up on demand.

```java
try (var options = Options.newOptions();
     var secondary = RocksDB.openSecondary(options, primaryPath, secondaryPath)) {
	secondary.tryCatchUpWithPrimary();
	byte[] value = secondary.get("k".getBytes());
}
```

`secondaryPath` is a private scratch directory for the secondary's own info logs; it is not a copy
of the data. The secondary only sees data the primary has flushed to SST files or WAL.

## Store large values in blob files

Blob storage keeps large values out of the LSM tree, so compaction moves keys instead of payloads.

```java
try (var options = Options.newOptions()
		.setCreateIfMissing(true)
		.setEnableBlobFiles(true)
		.setMinBlobSize(MemorySize.ofKB(4))
		.setBlobFileSize(MemorySize.ofMB(256))
		.setBlobCompressionType(CompressionType.ZSTD)
		.setEnableBlobGc(true);
     var db = RocksDB.openBlob(options, dbPath)) {
	db.put("k".getBytes(), largeValue);
}
```

Values below `minBlobSize` stay inline in the SST. Blob activity is visible through
`Property.BLOB_STATS` and `Property.NUM_BLOB_FILES`.

## Tune the block cache and bloom filters

```java
try (var cache = LRUCache.newLRUCache(MemorySize.ofMB(512));
     var tableConfig = BlockBasedTableOptions.newBlockBasedConfig()
		     .setBlockCache(cache)
		     .setBlockSize(MemorySize.ofKB(16))
		     .setFilterPolicy(FilterPolicy.newBloom(10))
		     .setCacheIndexAndFilterBlocks(true);
     var options = Options.newOptions()
		     .setCreateIfMissing(true)
		     .setTableFormatConfig(tableConfig);
     var db = RocksDB.openReadWrite(options, dbPath)) {
	// ...
}
```

`HyperClockCache.newHyperClockCache(capacity, estimatedEntryCharge)` is the alternative to
`LRUCache`, designed to scale better under heavy concurrency; pass `MemorySize.ZERO` as the charge
to let RocksDB estimate it. `FilterPolicy.newRibbon(10)` is the Bloom successor — better space
efficiency at a similar query cost and the same false-positive rate.

`setFilterPolicy` and `setBlockCache` transfer ownership to the table options; `setTableFormatConfig`
transfers the table options to `Options`. Keeping them in the try-with-resources block is still
correct — a transferred object's `close()` is a no-op.

## Throttle background I/O

```java
try (var rateLimiter = RateLimiter.create(MemorySize.ofMB(10));   // 10 MB/s
     var options = Options.newOptions()
		     .setCreateIfMissing(true)
		     .setRateLimiter(rateLimiter);
     var db = RocksDB.openReadWrite(options, dbPath)) {
	// ...
}
```

`RateLimiter.createAutoTuned(...)` lets RocksDB adjust the limit to the observed workload;
`RateLimiter.createWithMode(...)` selects whether reads, writes, or all I/O are limited.

## Cap disk usage

```java
try (var env = Env.defaultEnv();
     var sstFileManager = SstFileManager.create(env)
		     .setMaxAllowedSpaceUsage(MemorySize.ofGB(10))
		     .setDeleteRateBytesPerSecond(MemorySize.ofMB(64));
     var options = Options.newOptions()
		     .setCreateIfMissing(true)
		     .setSstFileManager(sstFileManager);
     var db = RocksDB.openReadWrite(options, dbPath)) {

	if (sstFileManager.isMaxAllowedSpaceReached()) {
		// writes are now failing with RocksDBException
	}
}
```

Throttling deletions keeps file removal from starving foreground I/O on slow disks.

## Inspect properties and statistics

Properties are per-database counters and reports, typed by the `Property` enum:

```java
Optional<String> stats = db.getProperty(Property.STATS);
OptionalLong keys = db.getLongProperty(Property.ESTIMATE_NUM_KEYS);
```

Statistics are opt-in on `Options` and survive for the life of the database:

```java
try (var options = Options.newOptions()
		.setCreateIfMissing(true)
		.enableStatistics()
		.setStatisticsLevel(StatsLevel.ALL);
     var db = RocksDB.openReadWrite(options, dbPath);
     var histogram = StatisticsHistogramData.newStatisticsHistogramData()) {

	db.get("k".getBytes());

	long hits = options.getTickerCount(TickerType.BLOCK_CACHE_HIT);
	options.getHistogramData(HistogramType.DB_GET, histogram);
	System.out.println(histogram.getP99() + " µs at p99");
}
```

## Profile a single operation

`PerfContext` is thread-local and measures the calling thread only.

```java
PerfContext.setPerfLevel(PerfLevel.ENABLE_COUNT);
try (var perf = PerfContext.newPerfContext()) {     // resets the counters
	db.get("k".getBytes());

	long comparisons = perf.metric(PerfMetric.USER_KEY_COMPARISON_COUNT);
	long cacheHits = perf.metric(PerfMetric.BLOCK_CACHE_HIT_COUNT);
	System.out.println(perf.report(true));          // true = skip zero counters
} finally {
	PerfContext.setPerfLevel(PerfLevel.DISABLE);
}
```

`PerfContext.currentPerfContext()` reads the counters without resetting them. `ENABLE_TIME` adds
timing at a measurable cost — leave the level at `DISABLE` in production.

## Route RocksDB logs into your logger

```java
try (var logger = Logger.newCallbackLogger(LogLevel.INFO,
		(level, message) -> slf4jLogger.info("[rocksdb {}] {}", level, message));
     var options = Options.newOptions()
		     .setCreateIfMissing(true)
		     .setInfoLog(logger)
		     .setInfoLogLevel(LogLevel.INFO);
     var db = RocksDB.openReadWrite(options, dbPath)) {
	// ...
}
```

The callback runs on RocksDB's own threads and **must not throw**. `Logger.newStderrLogger(level,
prefix)` is the zero-setup alternative.

## Observe flushes and compactions

Implement only the `EventNotifier` methods you care about — all eight have no-op defaults.

```java
EventNotifier notifier = new EventNotifier() {
	@Override
	public void onFlushCompleted(FlushJobInfo info) {
		metrics.flushCompleted(info.columnFamilyName(), info.flushReason());
	}

	@Override
	public void onCompactionCompleted(CompactionJobInfo info) {
		metrics.compactionCompleted(info.outputLevel(), info.compactionReason());
	}

	@Override
	public void onStallConditionsChanged(WriteStallInfo info) {
		metrics.writeStall(info.previous(), info.current());
	}
};

try (var options = Options.newOptions()
		.setCreateIfMissing(true)
		.addEventListener(notifier);
     var db = RocksDB.openReadWrite(options, dbPath)) {
	// ...
}
```

Three rules, all consequences of these callbacks running on RocksDB's background threads rather
than yours:

- **Be thread-safe.** Several background threads can be inside your notifier at once.
- **Do not throw.** An escaping exception is caught and logged, never propagated into native code.
- **Do not retain the `*Info` argument.** It is a zero-copy view over memory RocksDB owns only for
  the duration of the call. Copy out the fields you need, as above.

`addEventListener` may be called repeatedly to register several independent notifiers.

Registering a notifier also installs a JVM shutdown hook that stops RocksDB's background threads
before the process exits; without it `System.exit()` would deadlock. You do not need to do
anything, but see
[explanation.md#background-thread-callbacks](explanation.md#background-thread-callbacks) if you
run your own shutdown sequencing.

## Compact manually

```java
db.compactRange();                                             // whole keyspace
db.compactRange("a".getBytes(), "m".getBytes());               // a subrange
db.suggestCompactRange("a".getBytes(), "m".getBytes());        // hint, returns immediately

try (var compactOptions = CompactOptions.newCompactOptions()
		.setChangeLevel(true)
		.setTargetLevel(3)) {
	db.compactRange(compactOptions, "a".getBytes(), "m".getBytes());
}
```

`compactRange` blocks until done. To wait for *background* compaction to settle — before a clean
shutdown, for example:

```java
try (var waitOptions = WaitForCompactOptions.create()
		.setFlush(true)
		.setTimeout(Duration.ofMinutes(5))) {
	db.waitForCompact(waitOptions);
}
```

`db.disableFileDeletions()` / `enableFileDeletions()` bracket an external copy of the live files.

## Load a custom native library

Point the loader at a library on disk instead of the bundled classpath resource — useful for
bisecting a RocksDB build or testing a patched one:

```
java -Drocksdb.lib.path=/path/to/librocksdb.so ...
```

Details in [explanation.md#native-library-loading](explanation.md#native-library-loading).

## Build the native library from source

Only needed when working on this repository:

```bash
git submodule update --init --recursive     # first time
./mvnw generate-resources -Pnative-build    # builds librocksdb for this platform
./mvnw test
```

This needs a JDK 25+, [Zig](https://ziglang.org/) 0.15.x, and — for the Windows targets only —
CMake plus `make` or Ninja. Use `./mvnw`, never a system `mvn`, and never `install` (it pollutes
`~/.m2` with local artifacts).

## Run the benchmarks

```bash
./mvnw test-compile -q
./scripts/benchmark.sh
```

The script runs the FFM and JNI JMH suites and prints a side-by-side table. Published results and
the methodology caveats are in [benchmarks.md](benchmarks.md).
