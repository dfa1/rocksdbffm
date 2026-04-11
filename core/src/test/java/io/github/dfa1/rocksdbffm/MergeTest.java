package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class MergeTest {

	/// Opens a DB configured with the uint64add merge operator.
	/// Values are little-endian uint64_t; merge operands are added together.
	private static ReadWriteDB openWithMerge(Path dir) {
		try (Options opts = Options.newOptions()
				.setCreateIfMissing(true)
				.setUInt64AddMergeOperator()) {
			return RocksDB.open(opts, dir);
		}
	}

	private static byte[] uint64LE(long value) {
		ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
		buf.putLong(value);
		return buf.array();
	}

	private static long readUint64LE(byte[] bytes) {
		return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
	}

	// -----------------------------------------------------------------------
	// byte[] overload
	// -----------------------------------------------------------------------

	@Test
	void merge_byteArray_accumulatesOperands(@TempDir Path dir) {
		// Given
		try (var db = openWithMerge(dir)) {
			db.put("counter".getBytes(), uint64LE(10));

			// When
			db.merge("counter".getBytes(), uint64LE(5));
			db.merge("counter".getBytes(), uint64LE(3));

			// Then
			byte[] result = db.get("counter".getBytes());
			assertThat(readUint64LE(result)).isEqualTo(18L);
		}
	}

	@Test
	void merge_byteArray_withoutBaseValue_treatsZeroAsBase(@TempDir Path dir) {
		// Given
		try (var db = openWithMerge(dir)) {

			// When — no prior put; uint64add treats missing key as 0
			db.merge("counter".getBytes(), uint64LE(42));

			// Then
			byte[] result = db.get("counter".getBytes());
			assertThat(readUint64LE(result)).isEqualTo(42L);
		}
	}

	// -----------------------------------------------------------------------
	// ByteBuffer overload
	// -----------------------------------------------------------------------

	@Test
	void merge_byteBuffer_accumulatesOperands(@TempDir Path dir) {
		// Given
		try (var db = openWithMerge(dir)) {
			db.put("counter".getBytes(), uint64LE(100));

			byte[] keyBytes = "counter".getBytes();
			ByteBuffer key = ByteBuffer.allocateDirect(keyBytes.length).put(keyBytes).flip();
			ByteBuffer operand = ByteBuffer.allocateDirect(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
			operand.putLong(7).flip();

			// When
			db.merge(key, operand);

			// Then
			assertThat(readUint64LE(db.get("counter".getBytes()))).isEqualTo(107L);
		}
	}

	@Test
	void merge_byteBuffer_multipleOperands(@TempDir Path dir) {
		// Given
		try (var db = openWithMerge(dir)) {
			db.put("n".getBytes(), uint64LE(0));

			byte[] keyBytes = "n".getBytes();

			// When
			for (int i = 1; i <= 5; i++) {
				ByteBuffer key = ByteBuffer.allocateDirect(keyBytes.length).put(keyBytes).flip();
				ByteBuffer operand = ByteBuffer.allocateDirect(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
				operand.putLong(i).flip();
				db.merge(key, operand);
			}

			// Then — 0 + 1 + 2 + 3 + 4 + 5 = 15
			assertThat(readUint64LE(db.get("n".getBytes()))).isEqualTo(15L);
		}
	}

	// -----------------------------------------------------------------------
	// MemorySegment overload
	// -----------------------------------------------------------------------

	@Test
	void merge_memorySegment_accumulatesOperands(@TempDir Path dir) {
		// Given
		try (var db = openWithMerge(dir);
		     Arena arena = Arena.ofConfined()) {

			db.put("counter".getBytes(), uint64LE(1000));

			MemorySegment key = arena.allocateFrom(ValueLayout.JAVA_BYTE, "counter".getBytes());
			MemorySegment operand = arena.allocate(ValueLayout.JAVA_LONG);
			operand.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, Long.reverseBytes(Long.reverseBytes(250)));

			// When
			db.merge(key, operand);

			// Then
			assertThat(readUint64LE(db.get("counter".getBytes()))).isEqualTo(1250L);
		}
	}

	@Test
	void merge_memorySegment_keyIsolatedFromOtherKeys(@TempDir Path dir) {
		// Given
		try (var db = openWithMerge(dir);
		     Arena arena = Arena.ofConfined()) {

			db.put("a".getBytes(), uint64LE(10));
			db.put("b".getBytes(), uint64LE(20));

			MemorySegment keyA = arena.allocateFrom(ValueLayout.JAVA_BYTE, "a".getBytes());
			MemorySegment operand = arena.allocate(ValueLayout.JAVA_LONG);
			operand.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, 5L);

			// When — merge only into "a"
			db.merge(keyA, operand);

			// Then
			assertThat(readUint64LE(db.get("a".getBytes()))).isEqualTo(15L);
			assertThat(readUint64LE(db.get("b".getBytes()))).isEqualTo(20L);
		}
	}
}
