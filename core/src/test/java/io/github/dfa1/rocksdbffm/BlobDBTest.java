package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class BlobDBTest {

	// -----------------------------------------------------------------------
	// get — byte[] tier
	// -----------------------------------------------------------------------

	@Test
	void get_returnsStoredValue(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openWithBlobFiles(dir)) {
			db.put("k".getBytes(), "v".getBytes());

			// When
			var result = db.get("k".getBytes());

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
		try (var db = RocksDB.openWithBlobFiles(dir)) {
			db.put("key".getBytes(), "value".getBytes());

			var key = ByteBuffer.allocateDirect(3);
			key.put("key".getBytes()).flip();
			var out = ByteBuffer.allocateDirect(32);

			// When
			CopyResult result = db.get(key, out);

			// Then
			assertThat(result).isEqualTo(new CopyResult.Copied());
			out.flip();
			var bytes = new byte[out.remaining()];
			out.get(bytes);
			assertThat(bytes).isEqualTo("value".getBytes());
		}
	}

	@Test
	void get_byteBuffer_returnsNotFound_whenKeyAbsent(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openWithBlobFiles(dir)) {
			db.put("seed".getBytes(), "val".getBytes());

			var key = ByteBuffer.allocateDirect(7);
			key.put("missing".getBytes()).flip();
			var out = ByteBuffer.allocateDirect(32);

			// When
			CopyResult result = db.get(key, out);

			// Then
			assertThat(result).isEqualTo(new CopyResult.NotFound());
		}
	}

	@Test
	void get_byteBuffer_returnsNotEnoughCapacity_whenValueDoesNotFit(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openWithBlobFiles(dir)) {
			db.put("key".getBytes(), "value".getBytes());

			var key = ByteBuffer.allocateDirect(3);
			key.put("key".getBytes()).flip();
			var out = ByteBuffer.allocateDirect(2);

			// When
			CopyResult result = db.get(key, out);

			// Then
			assertThat(result).isEqualTo(new CopyResult.NotEnoughCapacity(5));
			assertThat(out.position()).isZero();
		}
	}

	// -----------------------------------------------------------------------
	// get — scoped zero-copy (Mapper)
	// -----------------------------------------------------------------------

	@Test
	void get_zeroCopy_returnsValue(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openWithBlobFiles(dir);
		     Arena arena = Arena.ofConfined()) {
			db.put("k".getBytes(), "v".getBytes());
			var key = arena.allocateFrom("k").asSlice(0, 1);

			// When
			var result = db.get(key, value -> value.toArray(ValueLayout.JAVA_BYTE));

			// Then
			assertThat(result).isPresent();
			assertThat(result.get()).isEqualTo("v".getBytes());
		}
	}

	@Test
	void get_zeroCopy_returnsEmpty_whenKeyAbsent(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openWithBlobFiles(dir);
		     Arena arena = Arena.ofConfined()) {
			var key = arena.allocateFrom("missing").asSlice(0, 7);

			// When
			var result = db.get(key, value -> value.toArray(ValueLayout.JAVA_BYTE));

			// Then
			assertThat(result).isEmpty();
		}
	}
}
