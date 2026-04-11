package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LoggerTest {

	@Test
	void stderrLogger_canBeCreatedAndClosed() {
		// Given / When / Then — no exception
		try (var logger = Logger.newStderrLogger(LogLevel.WARN, "test")) {
			assertThat(logger).isNotNull();
		}
	}

	@Test
	void stderrLogger_withNullPrefix() {
		try (var logger = Logger.newStderrLogger(LogLevel.ERROR, null)) {
			assertThat(logger).isNotNull();
		}
	}

	@Test
	void callbackLogger_receivesMessages(@TempDir Path dir) {
		// Given
		List<String> messages = new ArrayList<>();
		List<LogLevel> levels = new ArrayList<>();

		try (var logger = Logger.newCallbackLogger(LogLevel.DEBUG, (level, msg) -> {
			levels.add(level);
			messages.add(msg);
		});
		     var opts = Options.newOptions().setCreateIfMissing(true).setInfoLog(logger);
		     var db = RocksDB.open(opts, dir)) {

			// When — trigger at least one internal log message via a flush
			db.put("k".getBytes(), "v".getBytes());
		}

		// Then — at least one message should have been delivered
		assertThat(messages).isNotEmpty();
		assertThat(levels).isNotEmpty();
		assertThat(levels).allSatisfy(l -> assertThat(l).isNotNull());
	}

	@Test
	void setInfoLogLevel_roundTrips() {
		// Given / When / Then
		try (var opts = Options.newOptions()) {
			opts.setInfoLogLevel(LogLevel.WARN);
			assertThat(opts.getInfoLogLevel()).isEqualTo(LogLevel.WARN);

			opts.setInfoLogLevel(LogLevel.ERROR);
			assertThat(opts.getInfoLogLevel()).isEqualTo(LogLevel.ERROR);
		}
	}

	@Test
	void callbackLogger_canBeClosedAfterPassedToOptions(@TempDir Path dir) {
		// Given — close the logger before the DB, to verify RocksDB holds its own reference
		List<String> messages = new ArrayList<>();
		Logger logger = Logger.newCallbackLogger(LogLevel.DEBUG, (level, msg) -> messages.add(msg));

		try (var opts = Options.newOptions().setCreateIfMissing(true).setInfoLog(logger)) {
			logger.close(); // safe: RocksDB still holds a shared_ptr reference

			// When
			try (var db = RocksDB.open(opts, dir)) {
				db.put("k".getBytes(), "v".getBytes());
			}
		}

		// Then — no crash; messages may or may not arrive after the upcall arena is closed
		// (just verify it doesn't throw)
	}
}
