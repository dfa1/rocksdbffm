package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class EnvTest {

	// -----------------------------------------------------------------------
	// defaultEnv
	// -----------------------------------------------------------------------

	@Test
	void defaultEnv_createsAndCloses_doesNotThrow() {
		// Given — no preconditions

		// When / Then
		assertThatNoException().isThrownBy(() -> {
			try (var env = Env.defaultEnv()) {
				assertThat(env).isNotNull();
			}
		});
	}

	// -----------------------------------------------------------------------
	// memEnv
	// -----------------------------------------------------------------------

	@Test
	void memEnv_createsAndCloses_doesNotThrow() {
		// Given — no preconditions

		// When / Then
		assertThatNoException().isThrownBy(() -> {
			try (var env = Env.memEnv()) {
				assertThat(env).isNotNull();
			}
		});
	}

	// -----------------------------------------------------------------------
	// backgroundThreads round-trip
	// -----------------------------------------------------------------------

	@Test
	void backgroundThreads_setAndGet_roundTrips() {
		// Given
		try (var env = Env.defaultEnv()) {

			// When
			env.setBackgroundThreads(4);
			int result = env.getBackgroundThreads();

			// Then
			assertThat(result).isEqualTo(4);
		}
	}

	@Test
	void backgroundThreads_chaining_returnsSelf() {
		// Given
		try (var env = Env.defaultEnv()) {

			// When
			var returned = env.setBackgroundThreads(2);

			// Then
			assertThat(returned).isSameAs(env);
		}
	}

	// -----------------------------------------------------------------------
	// highPriorityBackgroundThreads round-trip
	// -----------------------------------------------------------------------

	@Test
	void highPriorityBackgroundThreads_setAndGet_roundTrips() {
		// Given
		try (var env = Env.defaultEnv()) {

			// When
			env.setHighPriorityBackgroundThreads(2);
			int result = env.getHighPriorityBackgroundThreads();

			// Then
			assertThat(result).isEqualTo(2);
		}
	}

	@Test
	void highPriorityBackgroundThreads_chaining_returnsSelf() {
		// Given
		try (var env = Env.defaultEnv()) {

			// When
			var returned = env.setHighPriorityBackgroundThreads(1);

			// Then
			assertThat(returned).isSameAs(env);
		}
	}

	// -----------------------------------------------------------------------
	// Options.setEnv integration
	// -----------------------------------------------------------------------

	@Test
	void setEnv_withDefaultEnv_opensDb(@TempDir Path dir) {
		// Given
		try (var env = Env.defaultEnv();
		     var opts = Options.newOptions().setCreateIfMissing(true).setEnv(env)) {

			// When
			assertThatNoException().isThrownBy(() -> {
				try (var db = RocksDB.open(opts, dir)) {
					db.put("k".getBytes(), "v".getBytes());
					byte[] result = db.get("k".getBytes());
					assertThat(result).isEqualTo("v".getBytes());
				}
			});

			// Then — no exception
		}
	}

	@Test
	void setEnv_withMemEnv_opensDb(@TempDir Path dir) {
		// Given
		try (var env = Env.memEnv();
		     var opts = Options.newOptions().setCreateIfMissing(true).setEnv(env)) {

			// When
			assertThatNoException().isThrownBy(() -> {
				try (var db = RocksDB.open(opts, dir)) {
					db.put("key".getBytes(), "value".getBytes());
					byte[] result = db.get("key".getBytes());
					assertThat(result).isEqualTo("value".getBytes());
				}
			});

			// Then — no exception
		}
	}
}
