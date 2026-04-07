package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class KeyMayExistTest {

	// -----------------------------------------------------------------------
	// byte[] variants
	// -----------------------------------------------------------------------

	@Test
	void keyMayExist_returnsFalse_forAbsentKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {

			// When / Then — Bloom filter must rule out a key that was never written
			assertThat(db.keyMayExist("ghost".getBytes())).isFalse();
		}
	}

	@Test
	void keyMayExist_returnsTrue_forPresentKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), "v".getBytes());

			// When / Then
			assertThat(db.keyMayExist("k".getBytes())).isTrue();
		}
	}

	@Test
	void keyMayExist_mayReturnTrue_afterDelete(@TempDir Path dir) {
		// Given — Bloom filters are additive; a deleted key may still pass the filter.
		// This test just asserts no exception is thrown and the result is a boolean.
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), "v".getBytes());
			db.delete("k".getBytes());

			// When / Then — result is unspecified but must not throw
			db.keyMayExist("k".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// ReadOptions overload (snapshot-pinned check)
	// -----------------------------------------------------------------------

	@Test
	void keyMayExist_withReadOptions_snapshot(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			try (Snapshot snap = db.getSnapshot();
			     ReadOptions ro = ReadOptions.newReadOptions().setSnapshot(snap)) {

				db.put("added-after-snap".getBytes(), "v".getBytes());

				// When — key written after the snapshot
				boolean result = db.keyMayExist(ro, "added-after-snap".getBytes());

				// Then — Bloom filter may or may not say present; just must not throw
				// (Bloom filters are not snapshot-aware at the filter level, but the
				//  subsequent Get would respect the snapshot boundary)
				assertThat(result).isIn(true, false);
			}
		}
	}

	// -----------------------------------------------------------------------
	// ByteBuffer variant
	// -----------------------------------------------------------------------

	@Test
	void keyMayExist_byteBuffer_returnsFalse_forAbsentKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			ByteBuffer key = ByteBuffer.allocateDirect(5);
			key.put("ghost".getBytes()).flip();

			// When / Then
			assertThat(db.keyMayExist(key)).isFalse();
		}
	}

	@Test
	void keyMayExist_byteBuffer_returnsTrue_forPresentKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("hello".getBytes(), "world".getBytes());
			ByteBuffer key = ByteBuffer.allocateDirect(5);
			key.put("hello".getBytes()).flip();

			// When / Then
			assertThat(db.keyMayExist(key)).isTrue();
		}
	}

	// -----------------------------------------------------------------------
	// MemorySegment variant
	// -----------------------------------------------------------------------

	@Test
	void keyMayExist_memorySegment_returnsFalse_forAbsentKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     Arena arena = Arena.ofConfined()) {
			MemorySegment key = arena.allocateFrom("ghost");
			// allocateFrom adds a null terminator — use reinterpret to strip it
			MemorySegment keyNoNul = key.reinterpret(5);

			// When / Then
			assertThat(db.keyMayExist(keyNoNul)).isFalse();
		}
	}

	@Test
	void keyMayExist_memorySegment_returnsTrue_forPresentKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     Arena arena = Arena.ofConfined()) {
			db.put("hi".getBytes(), "there".getBytes());
			MemorySegment key = arena.allocate(2);
			key.setAtIndex(ValueLayout.JAVA_BYTE, 0, (byte) 'h');
			key.setAtIndex(ValueLayout.JAVA_BYTE, 1, (byte) 'i');

			// When / Then
			assertThat(db.keyMayExist(key)).isTrue();
		}
	}
}
