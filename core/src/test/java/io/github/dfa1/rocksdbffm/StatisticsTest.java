package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StatisticsTest {

	@TempDir
	Path tempDir;

	@Test
	public void testStatistics() {
		try (Options options = Options.newOptions()
				.setCreateIfMissing(true)
				.enableStatistics()
				.setStatisticsLevel(StatsLevel.ALL)) {

			assertEquals(StatsLevel.ALL, options.getStatisticsLevel());

			try (RocksDB db = RocksDB.open(options, tempDir)) {
				db.put("key1".getBytes(), "value1".getBytes());
				db.get("key1".getBytes());
				db.get("key2".getBytes()); // miss

				long putCount = options.getTickerCount(TickerType.NUMBER_KEYS_WRITTEN);
				assertTrue(putCount >= 1, "Put count should be at least 1, got " + putCount);

				long getCount = options.getTickerCount(TickerType.NUMBER_KEYS_READ);
				assertTrue(getCount >= 1, "Get count should be at least 1, got " + getCount);

				String stats = options.getStatisticsString();
				assertNotNull(stats);
				assertTrue(stats.contains("rocksdb.number.keys.written"));

				try (StatisticsHistogramData hist = StatisticsHistogramData.newStatisticsHistogramData()) {
					options.getHistogramData(HistogramType.DB_GET, hist);
					assertTrue(hist.getCount() >= 2, "Histogram count should be at least 2, got " + hist.getCount());
					assertTrue(hist.getAverage() >= 0);
				}
			}
		}
	}
}
