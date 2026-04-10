package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class ReadOnlyDBTest {

	// -----------------------------------------------------------------------
	// get — byte[] tier
	// -----------------------------------------------------------------------

	@Test
	void get_returnsStoredValue(@TempDir Path dir) {
		// Given
		try (var rw = RocksDB.open(dir)) {
			rw.put("k".getBytes(), "v".getBytes());
		}

		// When
		try (var ro = RocksDB.openReadOnly(dir)) {
			var result = ro.get("k".getBytes());

			// Then
			assertThat(result).isEqualTo("v".getBytes());
		}
	}

	@Test
	void get_returnsNull_whenKeyAbsent(@TempDir Path dir) {
		// Given
		try (var rw = RocksDB.open(dir)) {
			rw.put("seed".getBytes(), "val".getBytes());
		}

		try (var ro = RocksDB.openReadOnly(dir)) {

			// When
			var result = ro.get("nonexistent".getBytes());

			// Then
			assertThat(result).isNull();
		}
	}

	@Test
	void get_withReadOptions(@TempDir Path dir) {
		// Given
		try (var rw = RocksDB.open(dir)) {
			rw.put("k".getBytes(), "v".getBytes());
		}

		try (var ro = RocksDB.openReadOnly(dir);
		     var readOpts = ReadOptions.newReadOptions()) {

			// When
			var result = ro.get(readOpts, "k".getBytes());

			// Then
			assertThat(result).isEqualTo("v".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// get — ByteBuffer tier
	// -----------------------------------------------------------------------

	@Test
	void get_byteBuffer_returnsValue(@TempDir Path dir) {
		// Given
		try (var rw = RocksDB.open(dir)) {
			rw.put("key".getBytes(), "value".getBytes());
		}

		try (var ro = RocksDB.openReadOnly(dir)) {
			var key = ByteBuffer.allocateDirect(3);
			key.put("key".getBytes()).flip();
			var out = ByteBuffer.allocateDirect(32);

			// When
			int len = ro.get(key, out);

			// Then
			assertThat(len).isEqualTo(5);
			out.flip();
			var bytes = new byte[out.remaining()];
			out.get(bytes);
			assertThat(bytes).isEqualTo("value".getBytes());
		}
	}

	@Test
	void get_byteBuffer_returnsMinusOne_whenKeyAbsent(@TempDir Path dir) {
		// Given
		try (var rw = RocksDB.open(dir)) {
			rw.put("seed".getBytes(), "val".getBytes());
		}

		try (var ro = RocksDB.openReadOnly(dir)) {
			var key = ByteBuffer.allocateDirect(7);
			key.put("missing".getBytes()).flip();
			var out = ByteBuffer.allocateDirect(32);

			// When
			int len = ro.get(key, out);

			// Then
			assertThat(len).isEqualTo(-1);
		}
	}

	// -----------------------------------------------------------------------
	// get — MemorySegment tier
	// -----------------------------------------------------------------------

	@Test
	void get_memorySegment_returnsValue(@TempDir Path dir) {
		// Given
		try (var rw = RocksDB.open(dir)) {
			rw.put("key".getBytes(), "value".getBytes());
		}

		try (var ro = RocksDB.openReadOnly(dir);
		     Arena arena = Arena.ofConfined()) {
			var key = arena.allocateFrom("key");
			var value = arena.allocate(32);

			// When
			long len = ro.get(key.asSlice(0, 3), value);

			// Then
			assertThat(len).isEqualTo(5);
			assertThat(value.asSlice(0, 5).toArray(ValueLayout.JAVA_BYTE))
					.isEqualTo("value".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// Iterator
	// -----------------------------------------------------------------------

	@Test
	void newIterator_iteratesData(@TempDir Path dir) {
		// Given
		try (var rw = RocksDB.open(dir)) {
			rw.put("a".getBytes(), "1".getBytes());
			rw.put("b".getBytes(), "2".getBytes());
		}

		try (var ro = RocksDB.openReadOnly(dir)) {

			// When
			try (var it = ro.newIterator()) {
				it.seekToFirst();

				// Then
				assertThat(it.isValid()).isTrue();
				assertThat(it.key()).isEqualTo("a".getBytes());
				it.next();
				assertThat(it.isValid()).isTrue();
				assertThat(it.key()).isEqualTo("b".getBytes());
			}
		}
	}

	@Test
	void newIterator_withReadOptions(@TempDir Path dir) {
		// Given
		try (var rw = RocksDB.open(dir)) {
			rw.put("x".getBytes(), "y".getBytes());
		}

		try (var ro = RocksDB.openReadOnly(dir);
		     var readOpts = ReadOptions.newReadOptions()) {

			// When
			try (var it = ro.newIterator(readOpts)) {
				it.seekToFirst();

				// Then
				assertThat(it.isValid()).isTrue();
				assertThat(it.key()).isEqualTo("x".getBytes());
			}
		}
	}

	// -----------------------------------------------------------------------
	// Snapshot
	// -----------------------------------------------------------------------

	@Test
	void getSnapshot_allowsSnapshotRead(@TempDir Path dir) {
		// Given
		try (var rw = RocksDB.open(dir)) {
			rw.put("k".getBytes(), "v".getBytes());
		}

		try (var ro = RocksDB.openReadOnly(dir)) {

			// When
			try (var snap = ro.getSnapshot();
			     var readOpts = ReadOptions.newReadOptions().setSnapshot(snap)) {

				// Then
				assertThat(ro.get(readOpts, "k".getBytes())).isEqualTo("v".getBytes());
			}
		}
	}

	// -----------------------------------------------------------------------
	// DB Properties
	// -----------------------------------------------------------------------

	@Test
	void getLongProperty_returnsValue(@TempDir Path dir) {
		// Given
		try (var rw = RocksDB.open(dir)) {
			rw.put("k".getBytes(), "v".getBytes());
		}

		try (var ro = RocksDB.openReadOnly(dir)) {

			// When / Then
			assertThat(ro.getLongProperty(Property.ESTIMATE_NUM_KEYS)).isPresent();
		}
	}

	@Test
	void getProperty_returnsValue(@TempDir Path dir) {
		// Given
		try (var rw = RocksDB.open(dir)) {
			rw.put("k".getBytes(), "v".getBytes());
		}

		try (var ro = RocksDB.openReadOnly(dir)) {

			// When / Then
			assertThat(ro.getProperty(Property.STATS)).isPresent();
		}
	}
}
