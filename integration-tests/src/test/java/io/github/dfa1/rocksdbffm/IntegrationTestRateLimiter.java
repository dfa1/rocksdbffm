package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class IntegrationTestRateLimiter {

	@Test
	void openDbWithRateLimiter(@TempDir Path dir) {
		// Given
		try (var limiter = RateLimiter.create(MemorySize.ofMB(100));
		     var opts = Options.newOptions()
				     .setCreateIfMissing(true)
				     .setRateLimiter(limiter);
		     var db = RocksDB.open(opts, dir)) {

			// When
			db.put("key".getBytes(), "value".getBytes());

			// Then
			assertThat(db.get("key".getBytes())).isEqualTo("value".getBytes());
		}
	}

	@Test
	void autoTunedRateLimiter(@TempDir Path dir) {
		// Given
		try (var limiter = RateLimiter.createAutoTuned(MemorySize.ofMB(50));
		     var opts = Options.newOptions()
				     .setCreateIfMissing(true)
				     .setRateLimiter(limiter);
		     var db = RocksDB.open(opts, dir)) {

			// When
			db.put("k".getBytes(), "v".getBytes());

			// Then
			assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
		}
	}

	@Test
	void rateLimiterWithAllIoMode(@TempDir Path dir) {
		// Given
		try (var limiter = RateLimiter.createWithMode(
				MemorySize.ofMB(200), 100_000L, 10,
				RateLimiter.Mode.ALL_IO, false);
		     var opts = Options.newOptions()
				     .setCreateIfMissing(true)
				     .setRateLimiter(limiter);
		     var db = RocksDB.open(opts, dir)) {

			// When
			db.put("key".getBytes(), "value".getBytes());

			// Then
			assertThat(db.get("key".getBytes())).isEqualTo("value".getBytes());
		}
	}

	@Test
	void rateLimiterCanBeClosedBeforeOptions(@TempDir Path dir) {
		// Given — rate limiter uses shared ownership, safe to close before Options
		try (var limiter = RateLimiter.create(MemorySize.ofMB(100));
		     var opts = Options.newOptions()
				     .setCreateIfMissing(true)
				     .setRateLimiter(limiter)) {
			limiter.close();

			// When / Then — Options (and the underlying shared_ptr) still valid
			try (var db = RocksDB.open(opts, dir)) {
				db.put("key".getBytes(), "value".getBytes());
				assertThat(db.get("key".getBytes())).isEqualTo("value".getBytes());
			}
		}
	}
}
