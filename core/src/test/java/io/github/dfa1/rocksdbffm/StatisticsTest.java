package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class StatisticsTest {

	@TempDir
	Path tempDir;

	@Test
	void statistics() {
		try (Options options = Options.newOptions()
				.setCreateIfMissing(true)
				.enableStatistics()
				.setStatisticsLevel(StatsLevel.ALL)) {

			assertThat(options.getStatisticsLevel()).isEqualTo(StatsLevel.ALL);

			try (ReadWriteDB db = RocksDB.open(options, tempDir)) {
				db.put("key1".getBytes(), "value1".getBytes());
				db.get("key1".getBytes());
				db.get("key2".getBytes()); // miss

				String stats = options.getStatisticsString();

				assertThat(stats)
						.isNotNull()
						.contains("rocksdb.number.keys.written");

				try (StatisticsHistogramData hist = StatisticsHistogramData.newStatisticsHistogramData()) {
					options.getHistogramData(HistogramType.DB_GET, hist);
					assertThat(hist.getCount()).isGreaterThan(1);
					assertThat(hist.getAverage()).isGreaterThan(0);
				}
			}
		}
	}
}
