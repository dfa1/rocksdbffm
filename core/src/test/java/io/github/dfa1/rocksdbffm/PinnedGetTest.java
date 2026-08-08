package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PinnedGetTest {

	// -----------------------------------------------------------------------
	// Happy path / absent key
	// -----------------------------------------------------------------------

	@Test
	void pinnedGet_returnsValue_forExistingKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), "v".getBytes());

			try (Arena arena = Arena.ofConfined()) {
				MemorySegment key = arena.allocateFrom("k").asSlice(0, 1);

				// When
				var result = db.get(key, value -> value.toArray(ValueLayout.JAVA_BYTE));

				// Then
				assertThat(result).isPresent();
				assertThat(result.get()).isEqualTo("v".getBytes());
			}
		}
	}

	@Test
	void pinnedGet_returnsEmpty_forAbsentKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			try (Arena arena = Arena.ofConfined()) {
				MemorySegment key = arena.allocateFrom("missing").asSlice(0, 7);

				// When
				var result = db.get(key, value -> value.toArray(ValueLayout.JAVA_BYTE));

				// Then
				assertThat(result).isEmpty();
			}
		}
	}

	@Test
	void pinnedGet_emptyValue_doesNotThrow(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), new byte[0]);

			try (Arena arena = Arena.ofConfined()) {
				MemorySegment key = arena.allocateFrom("k").asSlice(0, 1);

				// When
				var result = db.get(key, MemorySegment::byteSize);

				// Then
				assertThat(result).contains(0L);
			}
		}
	}

	@Test
	void pinnedGet_mapperReturnsNull_throwsNullPointerException(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), "v".getBytes());

			try (Arena arena = Arena.ofConfined()) {
				MemorySegment key = arena.allocateFrom("k").asSlice(0, 1);

				// When / Then — a mapper that returns null must not be silently
				// conflated with Optional.empty() (which means "key not found")
				assertThatThrownBy(() -> db.get(key, value -> null))
						.isInstanceOf(NullPointerException.class);
			}
		}
	}

	// -----------------------------------------------------------------------
	// Lifetime enforcement — the whole point of this API
	// -----------------------------------------------------------------------

	@Test
	void pinnedGet_segmentUsedAfterReturn_throwsIllegalStateException(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), "v".getBytes());

			try (Arena arena = Arena.ofConfined()) {
				MemorySegment key = arena.allocateFrom("k").asSlice(0, 1);
				MemorySegment[] escaped = new MemorySegment[1];

				// When — smuggle the view out via a captured array, then use it after return
				db.get(key, value -> escaped[0] = value);

				// Then
				assertThatThrownBy(() -> escaped[0].get(ValueLayout.JAVA_BYTE, 0))
						.isInstanceOf(IllegalStateException.class);
			}
		}
	}

	@Test
	void pinnedGet_segmentUsedFromAnotherThread_throwsWrongThreadException(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), "v".getBytes());

			try (Arena arena = Arena.ofConfined()) {
				MemorySegment key = arena.allocateFrom("k").asSlice(0, 1);
				AtomicReference<Throwable> caughtOnOtherThread = new AtomicReference<>();

				// When — hand the view to another thread while the call is still in progress
				db.get(key, value -> {
					Thread other = new Thread(() -> {
						try {
							value.get(ValueLayout.JAVA_BYTE, 0);
						} catch (Throwable t) {
							caughtOnOtherThread.set(t);
						}
					});
					other.start();
					try {
						other.join();
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						throw new RuntimeException(e);
					}
					return true;
				});

				// Then
				assertThat(caughtOnOtherThread.get()).isInstanceOf(WrongThreadException.class);
			}
		}
	}

	// -----------------------------------------------------------------------
	// Exception propagation and cleanup
	// -----------------------------------------------------------------------

	@Test
	void pinnedGet_readerThrows_propagatesAndCleansUp(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), "v".getBytes());

			try (Arena arena = Arena.ofConfined()) {
				MemorySegment key = arena.allocateFrom("k").asSlice(0, 1);

				// When
				RuntimeException thrown = new RuntimeException("boom");

				// Then — the exception propagates out of get() unchanged
				assertThatThrownBy(() -> db.get(key, value -> {
					throw thrown;
				})).isSameAs(thrown);

				// And a later, independent call still succeeds — proving the handle and
				// arena from the failed call were destroyed/closed rather than leaked
				var result = db.get(key, value -> value.toArray(ValueLayout.JAVA_BYTE));
				assertThat(result).isPresent();
				assertThat(result.get()).isEqualTo("v".getBytes());
			}
		}
	}

	// -----------------------------------------------------------------------
	// Leak check — repeated calls must not exhaust native handles
	// -----------------------------------------------------------------------

	@Test
	void pinnedGet_manyIterations_doesNotLeakHandles(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), "v".getBytes());

			try (Arena arena = Arena.ofConfined()) {
				MemorySegment key = arena.allocateFrom("k").asSlice(0, 1);

				// When / Then — a leaked handle or arena per call would eventually crash
				// or exhaust memory; reaching the end without error is the signal.
				for (int i = 0; i < 50_000; i++) {
					var result = db.get(key, value -> value.get(ValueLayout.JAVA_BYTE, 0));
					assertThat(result).contains((byte) 'v');
				}
			}
		}
	}

	// -----------------------------------------------------------------------
	// Column family overload
	// -----------------------------------------------------------------------

	@Test
	void pinnedGetCf_returnsValue_forExistingKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"))) {
			db.put(cf, "k".getBytes(), "v".getBytes());

			try (Arena arena = Arena.ofConfined()) {
				MemorySegment key = arena.allocateFrom("k").asSlice(0, 1);

				// When
				var result = db.get(cf, key, value -> value.toArray(ValueLayout.JAVA_BYTE));

				// Then
				assertThat(result).isPresent();
				assertThat(result.get()).isEqualTo("v".getBytes());
			}
		}
	}

	@Test
	void pinnedGetCf_returnsEmpty_forAbsentKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"))) {

			try (Arena arena = Arena.ofConfined()) {
				MemorySegment key = arena.allocateFrom("missing").asSlice(0, 7);

				// When
				var result = db.get(cf, key, value -> value.toArray(ValueLayout.JAVA_BYTE));

				// Then
				assertThat(result).isEmpty();
			}
		}
	}

	@Test
	void pinnedGetCf_isIsolatedFromDefaultFamily(@TempDir Path dir) {
		// Given — same key written only into the non-default family
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"))) {
			db.put(cf, "k".getBytes(), "v".getBytes());

			try (Arena arena = Arena.ofConfined()) {
				MemorySegment key = arena.allocateFrom("k").asSlice(0, 1);

				// When
				var viaDefault = db.get(key, value -> value.toArray(ValueLayout.JAVA_BYTE));
				var viaCf = db.get(cf, key, value -> value.toArray(ValueLayout.JAVA_BYTE));

				// Then
				assertThat(viaDefault).isEmpty();
				assertThat(viaCf).isPresent();
			}
		}
	}

	// -----------------------------------------------------------------------
	// Default-CF equivalence — proves the two overloads target the right handles
	// -----------------------------------------------------------------------

	@Test
	void pinnedGet_and_pinnedGetCf_agreeOnDefaultFamily(@TempDir Path dir) {
		// Given
		List<ColumnFamilyHandle> handles = new ArrayList<>();
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.openWithColumnFamilies(opts, dir,
				     List.of(ColumnFamilyDescriptor.of("default")), handles)) {
			var defaultCf = handles.get(0);
			db.put("k".getBytes(), "v".getBytes());

			try (Arena arena = Arena.ofConfined()) {
				MemorySegment key = arena.allocateFrom("k").asSlice(0, 1);

				// When
				var viaPlain = db.get(key, value -> value.toArray(ValueLayout.JAVA_BYTE));
				var viaDefaultCf = db.get(defaultCf, key, value -> value.toArray(ValueLayout.JAVA_BYTE));

				// Then
				assertThat(viaPlain).isPresent();
				assertThat(viaDefaultCf).isPresent();
				assertThat(viaPlain.get()).isEqualTo(viaDefaultCf.get()).isEqualTo("v".getBytes());
			}

			defaultCf.close();
		}
	}
}
