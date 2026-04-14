package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class BackgroundJobsTest {

	// -----------------------------------------------------------------------
	// cancelAllBackgroundWork
	// -----------------------------------------------------------------------

	@Test
	void cancelAllBackgroundWork_noWait_doesNotThrow(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), "v".getBytes());

			// When
			assertThatNoException().isThrownBy(() -> db.cancelAllBackgroundWork(false));

			// Then — no exception
		}
	}

	@Test
	void cancelAllBackgroundWork_wait_doesNotThrow(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), "v".getBytes());

			// When
			assertThatNoException().isThrownBy(() -> db.cancelAllBackgroundWork(true));

			// Then — no exception
		}
	}

	// -----------------------------------------------------------------------
	// disableManualCompaction / enableManualCompaction
	// -----------------------------------------------------------------------

	@Test
	void disableAndEnableManualCompaction_doesNotThrow(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {

			// When
			assertThatNoException().isThrownBy(() -> {
				db.disableManualCompaction();
				db.enableManualCompaction();
			});

			// Then — no exception
		}
	}

	@Test
	void disableManualCompaction_doesNotPreventReads(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), "v".getBytes());
			db.disableManualCompaction();

			// When
			byte[] result = db.get("k".getBytes());

			// Then
			assertThat(result).isEqualTo("v".getBytes());

			db.enableManualCompaction();
		}
	}

	// -----------------------------------------------------------------------
	// waitForCompact
	// -----------------------------------------------------------------------

	@Test
	void waitForCompact_defaultOptions_doesNotThrow(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var opts = WaitForCompactOptions.create()) {

			db.put("k".getBytes(), "v".getBytes());

			// When
			assertThatNoException().isThrownBy(() -> db.waitForCompact(opts));

			// Then — no exception
		}
	}

	@Test
	void waitForCompact_withFlush_doesNotThrow(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var opts = WaitForCompactOptions.create().setFlush(true)) {

			db.put("k".getBytes(), "v".getBytes());

			// When
			assertThatNoException().isThrownBy(() -> db.waitForCompact(opts));

			// Then — no exception
		}
	}

	@Test
	void waitForCompact_withTimeout_doesNotThrow(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var opts = WaitForCompactOptions.create().setTimeout(Duration.ofSeconds(5))) {

			db.put("k".getBytes(), "v".getBytes());

			// When
			assertThatNoException().isThrownBy(() -> db.waitForCompact(opts));

			// Then — no exception
		}
	}

	// -----------------------------------------------------------------------
	// WaitForCompactOptions round-trips
	// -----------------------------------------------------------------------

	@Test
	void waitForCompactOptions_abortOnPause_roundTrips() {
		// Given
		try (var opts = WaitForCompactOptions.create()) {
			assertThat(opts.isAbortOnPause()).isFalse();

			// When
			opts.setAbortOnPause(true);

			// Then
			assertThat(opts.isAbortOnPause()).isTrue();

			// When
			opts.setAbortOnPause(false);

			// Then
			assertThat(opts.isAbortOnPause()).isFalse();
		}
	}

	@Test
	void waitForCompactOptions_flush_roundTrips() {
		// Given
		try (var opts = WaitForCompactOptions.create()) {
			assertThat(opts.isFlush()).isFalse();

			// When
			opts.setFlush(true);

			// Then
			assertThat(opts.isFlush()).isTrue();
		}
	}

	@Test
	void waitForCompactOptions_closeDb_roundTrips() {
		// Given
		try (var opts = WaitForCompactOptions.create()) {
			assertThat(opts.isCloseDb()).isFalse();

			// When
			opts.setCloseDb(true);

			// Then
			assertThat(opts.isCloseDb()).isTrue();
		}
	}

	@Test
	void waitForCompactOptions_timeout_roundTrips() {
		// Given
		try (var opts = WaitForCompactOptions.create()) {
			assertThat(opts.getTimeout()).isEqualTo(Duration.ZERO);

			// When
			opts.setTimeout(Duration.ofSeconds(10));

			// Then
			assertThat(opts.getTimeout()).isEqualTo(Duration.ofSeconds(10));
		}
	}

	@Test
	void waitForCompactOptions_chaining_setsAllFields() {
		// Given / When
		try (var opts = WaitForCompactOptions.create()
				.setAbortOnPause(true)
				.setFlush(true)
				.setCloseDb(false)
				.setTimeout(Duration.ofMillis(500))) {

			// Then
			assertThat(opts.isAbortOnPause()).isTrue();
			assertThat(opts.isFlush()).isTrue();
			assertThat(opts.isCloseDb()).isFalse();
			assertThat(opts.getTimeout()).isEqualTo(Duration.ofMillis(500));
		}
	}
}
