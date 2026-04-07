package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class TtlDBTest {

	// -----------------------------------------------------------------------
	// Basic open / read-back
	// -----------------------------------------------------------------------

	@Test
	void openWithTtl_storesAndRetrievesValues(@TempDir Path dir) {
		// Given / When
		try (var db = RocksDB.openWithTtl(dir, Duration.ofSeconds(60))) {
			db.put("k".getBytes(), "v".getBytes());

			// Then — key is readable immediately (TTL not yet elapsed)
			assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
		}
	}

	@Test
	void openWithTtl_withExplicitOptions(@TempDir Path dir) {
		// Given
		try (var opts = new Options().setCreateIfMissing(true);
		     var db = RocksDB.openWithTtl(opts, dir, Duration.ofMinutes(1))) {

			db.put("key".getBytes(), "value".getBytes());

			// When / Then
			assertThat(db.get("key".getBytes())).isEqualTo("value".getBytes());
		}
	}

	@Test
	void openWithTtl_zeroTtl_disablesExpiry(@TempDir Path dir) {
		// Given — TTL=0 means no expiry
		try (var db = RocksDB.openWithTtl(dir, Duration.ZERO)) {
			db.put("k".getBytes(), "v".getBytes());

			// Then — key survives indefinitely
			assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// Full API surface (TTL DB is a regular rocksdb_t*)
	// -----------------------------------------------------------------------

	@Test
	void openWithTtl_supportsDelete(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openWithTtl(dir, Duration.ofSeconds(60))) {
			db.put("k".getBytes(), "v".getBytes());

			// When
			db.delete("k".getBytes());

			// Then
			assertThat(db.get("k".getBytes())).isNull();
		}
	}

	@Test
	void openWithTtl_supportsWriteBatch(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openWithTtl(dir, Duration.ofSeconds(60));
		     var batch = WriteBatch.create()) {

			batch.put("k1".getBytes(), "v1".getBytes());
			batch.put("k2".getBytes(), "v2".getBytes());

			// When
			db.write(batch);

			// Then
			assertThat(db.get("k1".getBytes())).isEqualTo("v1".getBytes());
			assertThat(db.get("k2".getBytes())).isEqualTo("v2".getBytes());
		}
	}

	@Test
	void openWithTtl_supportsIterator(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openWithTtl(dir, Duration.ofSeconds(60))) {
			db.put("a".getBytes(), "1".getBytes());
			db.put("b".getBytes(), "2".getBytes());

			// When
			try (var it = db.newIterator()) {
				it.seekToFirst();

				// Then
				assertThat(it.isValid()).isTrue();
				assertThat(it.key()).isEqualTo("a".getBytes());
			}
		}
	}

	@Test
	void openWithTtl_surviveReopenWithSameTtl(@TempDir Path dir) {
		// Given — write, close, reopen, verify data persists
		try (var db = RocksDB.openWithTtl(dir, Duration.ofSeconds(60))) {
			db.put("persistent".getBytes(), "yes".getBytes());
		}

		// When
		try (var db = RocksDB.openWithTtl(dir, Duration.ofSeconds(60))) {

			// Then
			assertThat(db.get("persistent".getBytes())).isEqualTo("yes".getBytes());
		}
	}
}
