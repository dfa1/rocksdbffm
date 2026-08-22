# Reference

What exists and what it accepts. Look here for artifacts, classes, options, enums, and feature
status.

For task-oriented usage see [how-to.md](how-to.md); for design rationale see
[explanation.md](explanation.md). Method-level detail (parameters, exceptions, units) lives in the
Javadoc of each class; this page is the map.

- [Artifacts](#artifacts)
- [Entry points](#entry-points)
- [DB types](#db-types)
- [Key/value operations](#keyvalue-operations)
- [Options](#options)
- [Table options, caches, filters](#table-options-caches-filters)
- [WriteBatch](#writebatch)
- [Iterators](#iterators)
- [Snapshots](#snapshots)
- [Transactions](#transactions)
- [Column families](#column-families)
- [Backup, checkpoint, ingest](#backup-checkpoint-ingest)
- [WAL](#wal)
- [Compaction and background jobs](#compaction-and-background-jobs)
- [Observability](#observability)
- [Domain types](#domain-types)
- [Enums](#enums)
- [System properties](#system-properties)
- [Feature status](#feature-status)
- [C API gaps](c-api-gaps.md)

---

## Artifacts

Group id `io.github.dfa1`. Import the BOM once and omit versions everywhere else.

| Artifact                             | Contents                                     |
|:-------------------------------------|:---------------------------------------------|
| `rocksdbffm-bom`                     | Version management (`<type>pom</type>`, `<scope>import</scope>`) |
| `rocksdbffm-core`                    | Pure Java — the entire API                   |
| `rocksdbffm-native-osx-aarch64`      | `librocksdb.dylib` for macOS on Apple silicon |
| `rocksdbffm-native-linux-x86_64`     | `librocksdb.so` for Linux x86-64             |
| `rocksdbffm-native-linux-aarch64`    | `librocksdb.so` for Linux aarch64            |
| `rocksdbffm-native-windows-x86_64`   | `librocksdb.dll` for Windows x86-64          |
| `rocksdbffm-native-windows-aarch64`  | `librocksdb.dll` for Windows aarch64         |

Depend on `rocksdbffm-core` plus one native artifact per platform you ship to; declaring several is
normal and the loader picks the matching one at startup. Gradle:

```kotlin
implementation(platform("io.github.dfa1:rocksdbffm-bom:0.8"))
implementation("io.github.dfa1:rocksdbffm-core")
runtimeOnly("io.github.dfa1:rocksdbffm-native-linux-x86_64")
```

**Requirements:** JDK 25+. Launch with `--enable-native-access=ALL-UNNAMED` to silence the JDK's
restricted-method warning.

**SBOM:** a CycloneDX SBOM is published with every release. PURL:
`pkg:maven/io.github.dfa1/rocksdbffm-core@0.8`, which SCA tools (Syft, Grype, Trivy, osv.dev,
GitHub Advisory DB) use to identify the artifact.

## Entry points

Every database is opened through a static factory on `RocksDB`. There is no public constructor
anywhere in the library.

Column families are not a separate method name — every factory that supports them takes
`List<ColumnFamilyDescriptor>` / `List<ColumnFamilyHandle> out` as an overload of the plain form.

| Factory                                                                       | Column families? | Returns                   |
|:------------------------------------------------------------------------------|:-----------------|:--------------------------|
| `openReadWrite(Path)` / `(Options, Path)`                                      | yes               | `ReadWriteDB` (creates if missing) |
| `openReadOnly(Path)` / `(Options, Path)` / `(Options, Path, boolean errorIfWalFileExists)` | yes  | `ReadOnlyDB`  |
| `openTtl(Path, Duration)` / `(Options, Path, Duration)`                        | yes               | `TtlDB`                   |
| `openBlob(Path)` / `(Options, Path)`                                           | yes               | `BlobDB`                  |
| `openSecondary(Options, Path primary, Path secondary)`                         | yes               | `SecondaryDB`             |
| `openTransaction(Options, TransactionDBOptions, Path)`                         | yes               | `TransactionDB`           |
| `openOptimistic(Options, Path)`                                                | yes               | `OptimisticTransactionDB` |
| `listColumnFamilies(Options, Path)`                                            | —                 | `List<byte[]>`            |

`RocksDB` also exposes the FFM plumbing shared by every wrapper: `errHolder(Arena)`,
`checkError(MemorySegment)`, `toNative(Arena, byte[])`, `free(MemorySegment)`.

## DB types

Each type exposes only the operations that are valid for it — see
[explanation.md#only-valid-operations](explanation.md#only-valid-operations).

| Type                      | Write | Read | Iterate | Snapshot | Flush | Compact | Ingest | CF  |
|:--------------------------|:-----:|:----:|:-------:|:--------:|:-----:|:-------:|:------:|:---:|
| `ReadWriteDB`             |  ✅   |  ✅  |   ✅    |    ✅    |  ✅   |   ✅    |   ✅   | ✅  |
| `ReadOnlyDB`              |  —    |  ✅  |   ✅    |    ✅    |  —    |   —     |   —    | ✅  |
| `TtlDB`                   |  ✅   |  ✅  |   ✅    |    ✅    |  ✅   |   ✅    |   ✅   | ✅  |
| `BlobDB`                  |  ✅   |  ✅  |   ✅    |    ✅    |  ✅   |   ✅    |   ✅   | ✅  |
| `SecondaryDB`             |  —    |  ✅  |   ✅    |    ✅    |  —    |   —     |   —    | ✅  |
| `TransactionDB`           |  ✅   |  ✅  |   ✅    |    ✅    |  ✅   |   —     |   —    | ✅  |
| `OptimisticTransactionDB` |  ✅   |  ✅  |   ✅    |    ✅    |  ✅   |   ✅    |   ✅   | ✅  |

`SecondaryDB` adds `tryCatchUpWithPrimary()`. `TtlDB` adds `getTtl()`. `TransactionDB` and
`OptimisticTransactionDB` add `beginTransaction(...)`.

## Key/value operations

Three tiers everywhere, differing only in how bytes cross the boundary:

| Tier            | Write                                     | Read                                        | Cost                                    |
|:----------------|:------------------------------------------|:--------------------------------------------|:----------------------------------------|
| `byte[]`        | `put(byte[], byte[])`                     | `byte[] get(byte[])` — `null` if absent     | Allocates + copies both ways            |
| `ByteBuffer`    | `put(ByteBuffer, ByteBuffer)`             | `CopyResult get(ByteBuffer, ByteBuffer dst)`| One copy into your buffer; use direct buffers |
| `MemorySegment` | `put(MemorySegment, MemorySegment)`       | `CopyResult get(MemorySegment, MemorySegment dst)` | One copy; you own the arena      |

Other read/write methods on the read-write types:

| Method                                              | Notes                                                       |
|:----------------------------------------------------|:------------------------------------------------------------|
| `delete(key)`                                       | All three tiers                                              |
| `deleteRange(startKey, endKey)`                     | All three tiers; end key exclusive; one tombstone            |
| `keyMayExist(key)` / `keyMayExist(ReadOptions, key)`| Bloom-filter probe; `false` is definitive, `true` is a maybe |
| `get(ReadOptions, byte[])`                          | Read through a snapshot                                      |
| `write(WriteBatch)` / `write(Arena, WriteBatch)`    | Atomic apply                                                 |
| `put(Arena, ...)`                                   | Reuse a caller-supplied arena instead of an internal one     |

## Options

`Options.newOptions()`; all setters return `this`. Only read during `open` — close it afterwards.

| Method                                     | Type                    |
|:-------------------------------------------|:------------------------|
| `setCreateIfMissing` / `getCreateIfMissing` | `boolean`               |
| `setCompression` / `getCompression`         | `CompressionType`       |
| `setTableFormatConfig`                      | `BlockBasedTableOptions` (ownership transferred) |
| `enableStatistics`, `setStatisticsLevel` / `getStatisticsLevel` | `StatsLevel` |
| `getStatisticsString`, `getTickerCount(TickerType)`, `getHistogramData(HistogramType, StatisticsHistogramData)` | — |
| `setInfoLog`, `setInfoLogLevel` / `getInfoLogLevel` | `Logger`, `LogLevel` |
| `setEnv`                                    | `Env`                   |
| `setRateLimiter`                            | `RateLimiter`           |
| `setSstFileManager`                         | `SstFileManager`        |
| `setMetadataWriteTemperature` / `getMetadataWriteTemperature` | `Temperature` |
| `setWalWriteTemperature` / `getWalWriteTemperature` | `Temperature`   |
| `setLastLevelTemperature` / `getLastLevelTemperature` | `Temperature` |
| `setDefaultWriteTemperature` / `getDefaultWriteTemperature` | `Temperature` |
| `setDefaultTemperature` / `getDefaultTemperature` | `Temperature`      |

Blob options (used with `RocksDB.openBlob`, each with a matching getter):

| Method                         | Type                    |
|:-------------------------------|:------------------------|
| `setEnableBlobFiles`           | `boolean`               |
| `setMinBlobSize`               | `MemorySize`            |
| `setBlobFileSize`              | `MemorySize`            |
| `setBlobCompressionType`       | `CompressionType`       |
| `setEnableBlobGc`              | `boolean`               |
| `setBlobGcAgeCutoff`           | `Ratio`                 |
| `setBlobGcForceThreshold`      | `Ratio`                 |
| `setBlobCompactionReadaheadSize` | `MemorySize`          |
| `setBlobFileStartingLevel`     | `int`                   |
| `setBlobCache`                 | `Cache`                 |
| `setPrepopulateBlobCache`      | `PrepopulateBlobCache`  |

Per-call options:

| Class                            | Factory                                 | Setters                                                     |
|:---------------------------------|:----------------------------------------|:------------------------------------------------------------|
| `ReadOptions`                    | `newReadOptions()`                      | `setSnapshot(Snapshot)`, `setVerifyChecksums`, `setFillCache`, `setPinData`, `setTailing`, `setTotalOrderSeek`, `setPrefixSameAsStart`, `setReadaheadSize(MemorySize)`, `setIterateLowerBound`/`setIterateUpperBound` (byte[]/ByteBuffer/MemorySegment), `setRequestId(String)` |
| `WriteOptions`                   | `newWriteOptions()`                     | `setSync`, `setDisableWal`, `setIgnoreMissingColumnFamilies`, `setNoSlowdown`, `setLowPri`, `setMemtableInsertHintPerBatch`, `setRateLimiterPriority(IOPriority)`, `setIoActivity(IOActivity)` |
| `FlushOptions`                   | `newFlushOptions()`                     | `setWait(boolean)` / `isWait()`                             |
| `CompactOptions`                 | `newCompactOptions()`                   | `setExclusiveManualCompaction`, `setBottommostLevelCompaction`, `setChangeLevel`, `setTargetLevel` |
| `WaitForCompactOptions`          | `create()`                              | `setAbortOnPause`, `setFlush`, `setCloseDb`, `setTimeout(Duration)` |
| `IngestExternalFileOptions`      | `newIngestExternalFileOptions()`        | `setMoveFiles`, `setSnapshotConsistency`, `setAllowGlobalSeqno`, `setAllowBlockingFlush`, `setIngestBehind`, `setFailIfNotBottommostLevel` |
| `TransactionDBOptions`           | `newTransactionDBOptions()`             | `setMaxNumLocks(long)`, `setNumStripes(long)`               |
| `TransactionOptions`             | `newTransactionOptions()`               | `setSetSnapshot`, `setDeadlockDetect`, `setLockTimeout(Duration)` |
| `OptimisticTransactionOptions`   | `newOptimisticTransactionOptions()`     | `setSetSnapshot`                                            |
| `RestoreOptions`                 | `create()`                              | `setKeepLogFiles(boolean)`                                  |

## Table options, caches, filters

`BlockBasedTableOptions.newBlockBasedConfig()`:

| Method                        | Type                                 |
|:------------------------------|:-------------------------------------|
| `setBlockSize`                | `MemorySize`                         |
| `setBlockCache`               | `Cache` (ownership transferred)      |
| `setNoBlockCache`             | `boolean`                            |
| `setFilterPolicy`             | `FilterPolicy` (ownership transferred) |
| `setCacheIndexAndFilterBlocks`| `boolean`                            |
| `setIndexType`                | `BlockBasedTableOptions.IndexType`   |
| `setFormatVersion`            | `BlockBasedTableOptions.FormatVersion` |
| `setWholeKeyFiltering`        | `boolean`                            |
| `setPartitionFilters`         | `boolean`                            |

| Class              | Factory                                                                | Notes                                    |
|:-------------------|:-----------------------------------------------------------------------|:-----------------------------------------|
| `Cache` (abstract) | —                                                                      | `setCapacity`, `getCapacity`, `getUsage`, `getPinnedUsage` |
| `LRUCache`         | `newLRUCache(MemorySize)`                                              | Sharded LRU                              |
| `HyperClockCache`  | `newHyperClockCache(MemorySize, MemorySize charge[, int numShardBits])`| `MemorySize.ZERO` charge = auto-estimate; `-1` shard bits = auto |
| `FilterPolicy`     | `newBloom(double bitsPerKey)`, `newRibbon(double)`                     | Bloom `10` ≈ 1% false positives; Ribbon: better space efficiency at similar query cost |
| `RateLimiter`      | `create`, `createAutoTuned`, `createWithMode` (`READS_ONLY`, `WRITES_ONLY`, `ALL_IO`) | Rate given as `MemorySize` per second |
| `SstFileManager`   | `create(Env)`                                                          | `setMaxAllowedSpaceUsage`, `setCompactionBufferSize`, `setDeleteRateBytesPerSecond`, `setMaxTrashDbRatio`, `getTotalSize`, `getTotalTrashSize`, `isMaxAllowedSpaceReached` |
| `Env`              | `defaultEnv()`, `memEnv()`                                             | `setBackgroundThreads`, `setHighPriorityBackgroundThreads` |
| `Logger`           | `newStderrLogger(LogLevel, String prefix)`, `newCallbackLogger(LogLevel, LogCallback)` | Callback must not throw |

## WriteBatch

`WriteBatch.create()` — an atomic set of operations applied with `db.write(batch)`.

| Method                                  | Tiers                                |
|:----------------------------------------|:-------------------------------------|
| `put(key, value)`                       | `byte[]`, plus `put(Arena, byte[], byte[])` |
| `delete(key)`                           | `byte[]`                             |
| `deleteRange(startKey, endKey)`         | `byte[]`, `ByteBuffer`, `MemorySegment` |
| `put/delete/deleteRange(ColumnFamilyHandle, …)` | all three tiers              |
| `clear()`                               | Reuse the batch                      |
| `count()`                               | Number of buffered operations        |

## Iterators

`RocksIterator` from `db.newIterator()`, `newIterator(ReadOptions)`, `newIterator(cf)`, or
`newIterator(cf, ReadOptions)`.

| Group      | Methods                                                                       |
|:-----------|:-------------------------------------------------------------------------------|
| Position   | `seekToFirst()`, `seekToLast()`, `seek(target)`, `seekForPrev(target)`, `next()`, `prev()` — seeks accept all three tiers |
| State      | `isValid()`, `checkError()` (throws), `error()` → `Optional<String>`, `refresh()` |
| Zero copy  | `key(Mapper)`, `value(Mapper)` — view scoped to the callback                    |
| One copy   | `key(ByteBuffer)`, `value(ByteBuffer)` → `CopyResult`                           |
| Convenience| `key()`, `value()` → fresh `byte[]` per call                                    |

`isValid()` turning false means "end of range **or** error" — always call `checkError()` after the
loop.

## Snapshots

`db.getSnapshot()` returns a `Snapshot`; pass it to `ReadOptions.setSnapshot(...)` for reads and
iterators. `sequenceNumber()` gives the pinned `SequenceNumber`. Open snapshots hold back
compaction, so close them promptly.

## Transactions

| Type                                   | Conflict handling                                     |
|:---------------------------------------|:------------------------------------------------------|
| `TransactionDB` (pessimistic)          | Locks keys as they are touched; `beginTransaction(WriteOptions[, TransactionOptions])` |
| `OptimisticTransactionDB`              | No locks; conflicts throw from `commit()`; `beginTransaction(WriteOptions[, OptimisticTransactionOptions])` |

`Transaction` methods: `put`, `delete`, `get(ReadOptions, key)`,
`getForUpdate(ReadOptions, key, boolean exclusive)`, `newIterator(cf, ReadOptions)`, `getSnapshot()`,
`commit()`, `rollback()`, `setSavePoint()`, `rollbackToSavePoint()` — plus a
`ColumnFamilyHandle` overload of each data method.

## Column families

| Type                     | Description                                                        |
|:-------------------------|:-------------------------------------------------------------------|
| `ColumnFamilyDescriptor` | `of(String)`, `of(byte[])`, `of(name, Options)`; `name()`, `nameAsString()` |
| `ColumnFamilyHandle`     | Live handle; `getId()`, `getName()`; `AutoCloseable`               |

`createColumnFamily(ColumnFamilyDescriptor)` and `dropColumnFamily(handle)` exist on `ReadWriteDB`,
`TtlDB`, `BlobDB`, `TransactionDB`, and `OptimisticTransactionDB`. Every read method
(`get`/`keyMayExist`/`newIterator`/`getProperty`) has a first-argument `ColumnFamilyHandle`
overload across all three access tiers on those five types plus `ReadOnlyDB`; write methods
(`put`/`delete`/`deleteRange`/`flush`) add the same overload on the five writable types.
Reopening requires listing every existing column family, `default` included — for
`SecondaryDB`, via `openSecondary(Options, Path primary, Path secondary, List<ColumnFamilyDescriptor>, List<ColumnFamilyHandle>)`.

## Backup, checkpoint, ingest

| Type                  | API                                                                                              |
|:----------------------|:--------------------------------------------------------------------------------------------------|
| `Checkpoint`          | `newCheckpoint(db)` for any `RocksDBReadOperations` implementor (`ReadWriteDB`/`BlobDB`/`TtlDB`/`ReadOnlyDB`/`SecondaryDB`/`OptimisticTransactionDB`); `exportTo(Path[, MemorySize logSizeForFlush])` |
| `BackupEngine`        | `open(Options, Path)`, `open(BackupEngineOptions, Env)`; `createNewBackup(db[, boolean flushBeforeBackup])`, `getBackupInfo()`, `purgeOldBackups(int)`, `verifyBackup(BackupId)`, `restoreDbFromLatestBackup(...)`, `restoreDbFromBackup(BackupId, ...)` |
| `BackupEngineOptions` | `create(Path)`; `setShareTableFiles`, `setSync`, `setDestroyOldData`, `setBackupLogFiles`, `setBackupRateLimit`, `setRestoreRateLimit`, `setMaxBackgroundOperations`, `setCallbackTriggerIntervalSize`, `setMaxValidBackupsToOpen`, `setShareFilesWithChecksumNaming`, `setEnv` |
| `BackupInfo`          | Record: `backupId()`, `timestamp()` (epoch seconds), `size()` (`MemorySize`), `numberOfFiles()`   |
| `SstFileWriter`       | `newSstFileWriter(Options)`; `open(Path)`, `put`, `delete`, `deleteRange`, `finish()`, `fileSize()` — keys must be added in sorted order |
| Ingest                | `db.ingestExternalFile(Path \| List<Path>[, IngestExternalFileOptions])`                          |

## WAL

| Type              | API                                                                    |
|:------------------|:------------------------------------------------------------------------|
| `SequenceNumber`  | `db.getLatestSequenceNumber()`                                          |
| `WalIterator`     | `db.getUpdatesSince(SequenceNumber)`; `isValid()`, `next()`, `getBatch()`, `checkStatus()` |
| `WalBatchResult`  | Record: `sequenceNumber()`, `writeBatch()`; `AutoCloseable`             |

## Compaction and background jobs

| Method                                                        | Notes                                    |
|:---------------------------------------------------------------|:-----------------------------------------|
| `compactRange()`                                               | Entire keyspace, blocking                |
| `compactRange(start, end)`                                     | All three tiers                          |
| `compactRange(CompactOptions, byte[] start, byte[] end)`       | With level control                       |
| `suggestCompactRange(start, end)`                              | Hint; returns immediately                |
| `waitForCompact(WaitForCompactOptions)`                        | Wait for background compaction to settle |
| `cancelAllBackgroundWork(boolean wait)`                        | Shutdown path                            |
| `disableManualCompaction()` / `enableManualCompaction()`       | —                                        |
| `disableFileDeletions()` / `enableFileDeletions()`             | Bracket an external copy of live files   |
| `flush(FlushOptions)` / `flush(cf, FlushOptions)`              | Memtable → SST                           |
| `flushWal(boolean sync)`                                       | WAL only                                 |

## Observability

| Area          | API                                                                                       |
|:--------------|:-------------------------------------------------------------------------------------------|
| Properties    | `getProperty(Property)` → `Optional<String>`, `getLongProperty(Property)` → `OptionalLong`; `Property` has 79 constants, each `Type.STRING` or `Type.NUMERIC` (`propertyName()`, `type()`) |
| Statistics    | `Options.enableStatistics()`, `setStatisticsLevel(StatsLevel)`, `getStatisticsString()`, `getTickerCount(TickerType)` (235 tickers), `getHistogramData(HistogramType, StatisticsHistogramData)` (65 histograms) |
| Histogram data| `StatisticsHistogramData.newStatisticsHistogramData()`; `getMedian`, `getP95`, `getP99`, `getAverage`, `getStdDev`, `getMin`, `getMax`, `getCount`, `getSum` |
| Perf context  | `PerfContext.setPerfLevel(PerfLevel)`, `newPerfContext()` (resets), `currentPerfContext()` (does not reset), `metric(PerfMetric)` (78 metrics), `report(boolean excludeZeroCounters)`, `reset()` — thread-local |
| Logging       | `Options.setInfoLog(Logger)`, `setInfoLogLevel(LogLevel)`                                  |
| Event listeners| `Options.addEventListener(EventNotifier)` (callable repeatedly to register several); `EventNotifier` has 8 no-op default methods — `onFlushBegin`, `onFlushCompleted`, `onCompactionBegin`, `onCompactionCompleted`, `onExternalFileIngested`, `onBackgroundError`, `onStallConditionsChanged`, `onMemTableSealed`. Callbacks run on RocksDB background threads; the `*Info` arguments are zero-copy views valid only for the duration of the call — see [explanation.md#background-thread-callbacks](explanation.md#background-thread-callbacks) |
| Event payloads | `FlushJobInfo`, `CompactionJobInfo`, `ExternalFileIngestionInfo`, `MemTableInfo`, `WriteStallInfo` |

## Domain types

| Type              | Purpose                                                                                     |
|:------------------|:---------------------------------------------------------------------------------------------|
| `MemorySize`      | Byte counts. `ofBytes`, `ofKB`, `ofMB`, `ofGB`, `ZERO`, `toBytes()`; immutable, `Comparable` |
| `SequenceNumber`  | RocksDB sequence numbers. `of(long)`, `toLong()`, `isAfter`, `isBefore`; `Comparable`        |
| `BackupId`        | Backup identity (native `uint32`). `of(long)`, `toLong()`; `Comparable`                      |
| `Ratio`           | Fraction in `[0.0, 1.0]`. `of(double)`, `toDouble()`, `ZERO`, `ONE`; immutable, `Comparable` |
| `CopyResult`      | Sealed: `Copied()`, `NotEnoughCapacity(long required)`, `NotFound()`                         |
| `RocksDBException`| Unchecked; thrown for a genuine RocksDB-reported error (see [explanation.md#errors-are-always-loud](explanation.md#errors-are-always-loud)) |
| `NativeObject`    | Base class of every native wrapper; `ptr()`, `close()` (idempotent), abstract `tryClose`      |
| `NativeObjectWithChildren` | `NativeObject` subclass for wrappers that can produce children borrowing their pointer (every DB type producing `Snapshot`); `registerChild`/`unregisterChild`, closes children before `tryCloseResource` — see [explanation.md](explanation.md#lifecycle-and-ownership) |
| `NativeObjectWithBaseDb` | `NativeObjectWithChildren` subclass for wrappers with a second owned pointer (`TransactionDB`/`OptimisticTransactionDB`'s `rocksdb_t*` "base DB"); guarded `dbPtr()`, hooks `tryCloseBaseDb`/`tryClosePrimary` — see [explanation.md](explanation.md#lifecycle-and-ownership) |

`Path` is used for every filesystem argument; there is no `String` path overload anywhere.

## Enums

| Enum                                | Constants                                                                                     |
|:------------------------------------|:-----------------------------------------------------------------------------------------------|
| `CompressionType`                   | `NO_COMPRESSION`, `SNAPPY`, `ZLIB`, `BZLIB2`, `LZ4`, `LZ4HC`, `XPRESS`, `ZSTD`                 |
| `LogLevel`                          | `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL`, `HEADER`                                            |
| `StatsLevel`                        | `DISABLE_ALL`, `EXCEPT_TICKERS`, `EXCEPT_HISTOGRAM_OR_TIMERS`, `EXCEPT_TIMERS`, `EXCEPT_DETAILED_TIMERS`, `EXCEPT_TIME_FOR_MUTEX`, `ALL` |
| `PerfLevel`                         | `UNINITIALIZED`, `DISABLE`, `ENABLE_COUNT`, `ENABLE_TIME_EXCEPT_FOR_MUTEX`, `ENABLE_TIME`      |
| `PrepopulateBlobCache`              | `DISABLE`, `FLUSH_ONLY`                                                                        |
| `Temperature`                       | `UNKNOWN`, `HOT`, `WARM`, `COOL`, `COLD`, `ICE` — storage-tier hint, no-op for the default `FileSystem` |
| `RateLimiter.Mode`                  | `READS_ONLY`, `WRITES_ONLY`, `ALL_IO`                                                          |
| `IOPriority`                        | `LOW`, `MID`, `HIGH`, `USER`, `TOTAL`                                                          |
| `IOActivity`                        | `FLUSH`, `COMPACTION`, `DB_OPEN`, `GET`, `MULTI_GET`, `DB_ITERATOR`, `VERIFY_DB_CHECKSUM`, `VERIFY_FILE_CHECKSUMS`, `GET_ENTITY`, `MULTI_GET_ENTITY`, `GET_FILE_CHECKSUMS_FROM_CURRENT_MANIFEST`, `UNKNOWN` |
| `BlockBasedTableOptions.IndexType`  | Block-based index layout selection                                                             |
| `FlushReason`                       | Why a flush ran: `MANUAL_FLUSH`, `WRITE_BUFFER_FULL`, `WAL_FULL`, `AUTO_COMPACTION`, `ERROR_RECOVERY`, … (16 constants) |
| `CompactionReason`                  | Why a compaction ran: `MANUAL_COMPACTION`, `LEVEL_L0_FILES_NUM`, `TTL`, `BOTTOMMOST_FILES`, `PERIODIC_COMPACTION`, … (21 constants) |
| `BackgroundErrorReason`             | `FLUSH`, `COMPACTION`, `WRITE_CALLBACK`, `MEMTABLE`, `MANIFEST_WRITE`, `FLUSH_NO_WAL`, `MANIFEST_WRITE_NO_WAL`, `ASYNC_FILE_OPEN` |
| `WriteStallCondition`               | `NORMAL`, `DELAYED`, `STOPPED`                                                                 |
| `Property`, `TickerType`, `HistogramType`, `PerfMetric` | Large enumerations; see the Javadoc for the full lists                     |

`SNAPPY`, `ZLIB`, `BZLIB2`, and `XPRESS` aren't linked into the bundled `librocksdb` yet —
`Options.setCompression`/`setBlobCompressionType` reject them eagerly with
`UnsupportedOperationException` rather than deferring to a native failure at DB-open time.

## System properties

| Property             | Effect                                                                          |
|:---------------------|:---------------------------------------------------------------------------------|
| `rocksdb.lib.path`   | Load the native library from this absolute path instead of the classpath resource |

## Feature status

Parity tracking against `rocksdbjni`. ✅ implemented · 🚧 partial · ❌ not implemented yet ·
🚫 blocked on the C API (see [c-api-gaps.md](c-api-gaps.md)).

| Feature                    | Status | Notes                                                                                      |
|:---------------------------|:------:|:-------------------------------------------------------------------------------------------|
| DB open/create             |   ✅    | Options, createIfMissing, read-only                                                        |
| Put/Get/Delete             |   ✅    | All three tiers; zero-copy reads via `PinnableSlice`                                        |
| WriteBatch                 |   ✅    | Atomic multi-op writes                                                                      |
| Transactions (pessimistic) |   ✅    | `TransactionDB`, savepoints, get-for-update                                                 |
| Optimistic transactions    |   ✅    | Conflict detection at commit                                                                |
| Checkpoints                |   ✅    | Point-in-time on-disk snapshot                                                              |
| Table options              |   ✅    | `BlockBasedTableOptions`, `LRUCache`, `HyperClockCache`, `FilterPolicy` (Bloom, Ribbon)     |
| Iterators                  |   ✅    | seek/seekForPrev/next/prev; all three tiers                                                 |
| Snapshots                  |   ✅    | `ReadOptions.setSnapshot`, sequence numbers                                                 |
| Flush                      |   ✅    | `flush(FlushOptions)`, `flushWal(boolean)`                                                  |
| DB properties              |   ✅    | `getProperty`, `getLongProperty`                                                            |
| Statistics                 |   ✅    | `TickerType`, `HistogramType`, `StatsLevel`                                                 |
| Compression                |   ✅    | `CompressionType`; `Options.setCompression`                                                 |
| Column families            |   ✅    | Multi-CF open for every DB type; CF overloads on all data methods and `WriteBatch`          |
| DeleteRange                |   ✅    | Range tombstones on the DB and in `WriteBatch`; all three tiers                             |
| Compaction control         |   ✅    | `compactRange` (+`CompactOptions`), `suggestCompactRange`, file-deletion toggles            |
| Temperature hints          |   ✅    | `Temperature`; 5 `Options` setter/getter pairs (metadata/WAL/last-level/default-write/default). EXPERIMENTAL upstream, no-op for the default `FileSystem` |
| SST file ingest            |   ✅    | `SstFileWriter`, `ingestExternalFile`, `IngestExternalFileOptions`                          |
| Backup engine              |   ✅    | Incremental backup/restore, purge, verify                                                   |
| TTL DB                     |   ✅    | `openTtl(path, Duration)`; lazy expiry via compaction                                   |
| WAL iterator               |   ✅    | `getUpdatesSince`, `getLatestSequenceNumber` — CDC, replication, auditing                   |
| Rate limiter               |   ✅    | Writes-only, reads-only, all-IO; auto-tuned variant                                         |
| Env                        |   ✅    | `defaultEnv()`, `memEnv()`, background thread pools                                         |
| SST file manager           |   ✅    | Disk-space limits, trash-deletion rate, compaction buffer                                   |
| Secondary DB               |   ✅    | `tryCatchUpWithPrimary`, get, iterator, snapshot, properties                                |
| Blob DB                    |   ✅    | Blob options, blob properties, `PrepopulateBlobCache`                                       |
| Logger                     |   ✅    | Stderr and callback loggers                                                                 |
| Perf context               |   ✅    | `PerfContext`, `PerfLevel`, `PerfMetric`                                                    |
| Event listeners            |   ✅    | `EventNotifier` via `Options.addEventListener`; 8 of the C API's 10 callbacks (the two subcompaction ones are deliberately not exposed) |
| Background jobs            |   🚧    | `cancelAllBackgroundWork`, manual-compaction toggles, `waitForCompact`; Options-level tuning (FIFO/Universal) pending |
| MultiGet                   |   ❌    | `rocksdb_multi_get()` exists in the C API; no Java wrapper yet                              |
| Merge                      |   ✅    | `merge()` write op on all 7 write-capable types (byte[]/ByteBuffer/MemorySegment, CF variants), see [#8](https://github.com/dfa1/rocksdbffm/issues/8); requires a `MergeOperator` configured via `Options.setMergeOperator`, else calls fail with `RocksDBException` |
| MergeOperator               |   ✅    | `MergeOperator.uint64Add()` (built-in `rocksdb_options_set_uint64add_merge_operator`) and `MergeOperator.custom(String, FullMergeFn)` (`rocksdb_mergeoperator_create()` — `full_merge` implemented in Java, `partial_merge` always declines; `fn` receives zero-copy `MemorySegment` views of key/existing-value/operands instead of copied `byte[]`s, see [#94](https://github.com/dfa1/rocksdbffm/issues/94)) |
| CompactionFilter           |   ❌    | Callback-based custom compaction logic                                                      |
| Custom comparators         |   ❌    | `rocksdb_comparator_create()` exists in the C API                                           |
| Advanced column family     |   ❌    | Per-CF compaction style, level multipliers                                                  |
| Advanced memtable config   |   ❌    | SkipList tuning, hash-memtable variants                                                     |
| Persistent cache           |   🚫    | Not in `rocksdb/c.h` — C++ only (`NewPersistentCache`)                                      |
| Wide columns               |   🚫    | Not in `rocksdb/c.h` — C++ only (`PutEntity`, `GetEntity`)                                  |

The gap analysis — which features the C API exposes but this library has not wrapped, versus which
need an upstream contribution to `facebook/rocksdb` — is in [c-api-gaps.md](c-api-gaps.md).
