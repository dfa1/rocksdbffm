package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/// Every test here exercises a real RocksDB background-thread callback (flush, compaction, or
/// external-file ingestion), which permanently attaches a RocksDB background thread to the JVM.
/// That used to deadlock the Surefire fork on exit; see [BackgroundUpcallThreads] for why, and
/// [EventNotifierExitTest] for the regression test guarding the fix.
class EventNotifierTest {

	@Test
	void flush_firesBeginAndCompletedWithMatchingDetails(@TempDir Path dir) {
		// Given — a notifier that copies out the fields it needs, since FlushJobInfo is only
		// valid for the duration of each callback
		AtomicInteger beginCount = new AtomicInteger();
		List<String> completedCfNames = new CopyOnWriteArrayList<>();
		List<FlushReason> completedReasons = new CopyOnWriteArrayList<>();
		List<String> completedFilePaths = new CopyOnWriteArrayList<>();
		EventNotifier notifier = new EventNotifier() {
			@Override
			public void onFlushBegin(FlushJobInfo info) {
				beginCount.incrementAndGet();
			}

			@Override
			public void onFlushCompleted(FlushJobInfo info) {
				completedCfNames.add(info.columnFamilyName());
				completedReasons.add(info.flushReason());
				completedFilePaths.add(info.filePath().toString());
			}
		};

		try (var opts = Options.newOptions().setCreateIfMissing(true).addEventListener(notifier);
		     var db = RocksDB.openReadWrite(opts, dir)) {
			db.put("k".getBytes(), "v".getBytes());

			// When
			db.flush(FlushOptions.newFlushOptions());
		}

		// Then
		assertThat(beginCount.get()).isPositive();
		assertThat(completedCfNames).containsExactly("default");
		assertThat(completedReasons).containsExactly(FlushReason.MANUAL_FLUSH);
		assertThat(completedFilePaths.get(0)).endsWith(".sst");
	}

	@Test
	void flush_firesMemTableSealed(@TempDir Path dir) {
		// Given
		List<String> sealedCfNames = new CopyOnWriteArrayList<>();
		EventNotifier notifier = new EventNotifier() {
			@Override
			public void onMemTableSealed(MemTableInfo info) {
				sealedCfNames.add(info.columnFamilyName());
			}
		};

		try (var opts = Options.newOptions().setCreateIfMissing(true).addEventListener(notifier);
		     var db = RocksDB.openReadWrite(opts, dir)) {
			db.put("k".getBytes(), "v".getBytes());

			// When
			db.flush(FlushOptions.newFlushOptions());
		}

		// Then
		assertThat(sealedCfNames).containsExactly("default");
	}

	@Test
	void compactRange_firesCompactionCompletedWithMatchingDetails(@TempDir Path dir) {
		// Given — a couple of flushed SST files, so compactRange() has something to compact.
		// Kept below RocksDB's default level0_file_num_compaction_trigger (4) so the manual
		// compactRange() call below is the only thing that triggers a compaction.
		List<Integer> outputLevels = new CopyOnWriteArrayList<>();
		List<CompactionReason> reasons = new CopyOnWriteArrayList<>();
		List<RocksDBException> statuses = new CopyOnWriteArrayList<>();
		EventNotifier notifier = new EventNotifier() {
			@Override
			public void onCompactionCompleted(CompactionJobInfo info) {
				outputLevels.add(info.outputLevel());
				reasons.add(info.compactionReason());
				statuses.add(info.status());
			}
		};

		try (var opts = Options.newOptions().setCreateIfMissing(true).addEventListener(notifier);
		     var db = RocksDB.openReadWrite(opts, dir)) {
			for (int i = 0; i < 2; i++) {
				db.put(("k" + i).getBytes(), ("v" + i).getBytes());
				db.flush(FlushOptions.newFlushOptions());
			}

			// When
			db.compactRange();
		}

		// Then
		assertThat(outputLevels).isNotEmpty();
		assertThat(reasons).containsExactly(CompactionReason.MANUAL_COMPACTION);
		assertThat(statuses).allSatisfy(status -> assertThat(status).isNull());
	}

	@Test
	void ingestExternalFile_firesOnExternalFileIngested(@TempDir Path dir) {
		// Given
		Path sstPath = dir.resolve("data.sst");
		Path dbPath = dir.resolve("db");
		try (var writerOpts = Options.newOptions().setCreateIfMissing(true);
		     var writer = SstFileWriter.newSstFileWriter(writerOpts)) {
			writer.open(sstPath);
			writer.put("aaa".getBytes(), "val1".getBytes());
			writer.finish();
		}

		List<String> ingestedCfNames = new CopyOnWriteArrayList<>();
		EventNotifier notifier = new EventNotifier() {
			@Override
			public void onExternalFileIngested(ExternalFileIngestionInfo info) {
				ingestedCfNames.add(info.columnFamilyName());
			}
		};

		try (var opts = Options.newOptions().setCreateIfMissing(true).addEventListener(notifier);
		     var db = RocksDB.openReadWrite(opts, dbPath)) {

			// When
			db.ingestExternalFile(sstPath);
		}

		// Then
		assertThat(ingestedCfNames).containsExactly("default");
	}

	@Test
	void addEventListener_canBeCalledMultipleTimes(@TempDir Path dir) {
		// Given — two independent notifiers attached to the same Options
		AtomicInteger firstCount = new AtomicInteger();
		AtomicInteger secondCount = new AtomicInteger();
		EventNotifier first = new EventNotifier() {
			@Override
			public void onFlushCompleted(FlushJobInfo info) {
				firstCount.incrementAndGet();
			}
		};
		EventNotifier second = new EventNotifier() {
			@Override
			public void onFlushCompleted(FlushJobInfo info) {
				secondCount.incrementAndGet();
			}
		};

		try (var opts = Options.newOptions().setCreateIfMissing(true)
				.addEventListener(first).addEventListener(second);
		     var db = RocksDB.openReadWrite(opts, dir)) {
			db.put("k".getBytes(), "v".getBytes());

			// When
			db.flush(FlushOptions.newFlushOptions());
		}

		// Then
		assertThat(firstCount.get()).isPositive();
		assertThat(secondCount.get()).isPositive();
	}
}
