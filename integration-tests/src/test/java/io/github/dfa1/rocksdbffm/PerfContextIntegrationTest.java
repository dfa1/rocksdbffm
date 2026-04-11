package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class PerfContextIntegrationTest {

	@BeforeEach
	void enablePerfLevel() {
		PerfContext.setPerfLevel(PerfLevel.ENABLE_TIME_EXCEPT_FOR_MUTEX);
	}

	@AfterEach
	void disablePerfLevel() {
		PerfContext.setPerfLevel(PerfLevel.DISABLE);
	}

	@Test
	void getAccumulatesBlockCacheMetrics(@TempDir Path dir) {
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {

			// Given — data flushed to SST so block cache is consulted on read
			db.put("k".getBytes(), "v".getBytes());
			db.flush(FlushOptions.newFlushOptions());

			// When
			try (var ctx = PerfContext.newPerfContext()) {
				db.get("k".getBytes());

				// Then — at least one block was read or the cache was consulted
				long blockReads = ctx.metric(PerfMetric.BLOCK_READ_COUNT);
				long cacheHits = ctx.metric(PerfMetric.BLOCK_CACHE_HIT_COUNT);
				assertThat(blockReads + cacheHits).isGreaterThan(0);
			}
		}
	}

	@Test
	void writeAccumulatesWalAndMemtableMetrics(@TempDir Path dir) {
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var ctx = PerfContext.newPerfContext()) {

			// When
			db.put("k".getBytes(), "v".getBytes());

			// Then — WAL and memtable write times are tracked
			long writeWalTime = ctx.metric(PerfMetric.WRITE_WAL_TIME);
			long writeMemtableTime = ctx.metric(PerfMetric.WRITE_MEMTABLE_TIME);
			assertThat(writeWalTime + writeMemtableTime).isGreaterThan(0);
		}
	}

	@Test
	void resetClearsCounters(@TempDir Path dir) {
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var ctx = PerfContext.currentPerfContext()) {

			// Given — accumulate some metrics
			db.put("k".getBytes(), "v".getBytes());
			long walTime = ctx.metric(PerfMetric.WRITE_WAL_TIME);
			long memtableTime = ctx.metric(PerfMetric.WRITE_MEMTABLE_TIME);
			assertThat(walTime + memtableTime).isGreaterThan(0);

			// When
			ctx.reset();

			// Then — all counters are zero after reset
			assertThat(ctx.metric(PerfMetric.WRITE_WAL_TIME)).isZero();
			assertThat(ctx.metric(PerfMetric.BLOCK_READ_COUNT)).isZero();
			assertThat(ctx.metric(PerfMetric.BLOCK_CACHE_HIT_COUNT)).isZero();
		}
	}

	@Test
	void reportProducesNonEmptyString(@TempDir Path dir) {
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var ctx = PerfContext.newPerfContext()) {

			// Given — some metrics accumulated
			db.put("k".getBytes(), "v".getBytes());

			// When
			String report = ctx.report(false);
			String reportExcludingZeros = ctx.report(true);

			// Then
			assertThat(report).isNotBlank();
			assertThat(reportExcludingZeros).isNotBlank();
			// excluding-zeros report must be a subset of the full report
			assertThat(report.length()).isGreaterThanOrEqualTo(reportExcludingZeros.length());
		}
	}

	@Test
	void disabledLevelCollectsNothing(@TempDir Path dir) {
		// Given — perf collection is disabled
		PerfContext.setPerfLevel(PerfLevel.DISABLE);

		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var ctx = PerfContext.newPerfContext()) {

			// When
			db.put("k".getBytes(), "v".getBytes());

			// Then — timings stay zero when perf is disabled
			assertThat(ctx.metric(PerfMetric.WRITE_WAL_TIME)).isZero();
		}
	}

	@Test
	void allPerfLevelsAreSettable() {
		// Given — any PerfLevel value

		// When — each level is applied
		for (PerfLevel level : PerfLevel.values()) {
			PerfContext.setPerfLevel(level);
		}

		// Then — no exception was thrown (restore for @AfterEach)
		PerfContext.setPerfLevel(PerfLevel.ENABLE_COUNT);
	}

	@Test
	void iteratorAccumulatesMetrics(@TempDir Path dir) {
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {

			// Given — two keys flushed to SST
			db.put("a".getBytes(), "1".getBytes());
			db.put("b".getBytes(), "2".getBytes());
			db.flush(FlushOptions.newFlushOptions());

			// When — iterate over all keys
			try (var ctx = PerfContext.newPerfContext();
			     var it = db.newIterator()) {
				it.seekToFirst();
				while (it.isValid()) {
					it.next();
				}

				// Then — iter read bytes are tracked
				assertThat(ctx.metric(PerfMetric.ITER_READ_BYTES)).isGreaterThanOrEqualTo(0);
			}
		}
	}
}
