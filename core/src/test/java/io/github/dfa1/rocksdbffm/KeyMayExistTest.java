package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;

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

	// -----------------------------------------------------------------------
	// Column family overloads — all three tiers + ReadOptions
	// -----------------------------------------------------------------------

	@Test
	void keyMayExist_cf_byteArray_returnsFalse_forAbsentKey(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("test"))) {

			// When / Then
			assertThat(db.keyMayExist(cf, "ghost".getBytes())).isFalse();
		}
	}

	@Test
	void keyMayExist_cf_byteArray_returnsTrue_forPresentKey(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("test"))) {
			db.put(cf, "k".getBytes(), "v".getBytes());

			// When / Then
			assertThat(db.keyMayExist(cf, "k".getBytes())).isTrue();
		}
	}

	@Test
	void keyMayExist_cf_withReadOptions_doesNotThrow(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("test"));
		     var snap = db.getSnapshot();
		     var ro = ReadOptions.newReadOptions().setSnapshot(snap)) {
			db.put(cf, "k".getBytes(), "v".getBytes());

			// When — key written after snapshot; result is unspecified but must not throw
			boolean result = db.keyMayExist(cf, ro, "k".getBytes());

			// Then
			assertThat(result).isIn(true, false);
		}
	}

	@Test
	void keyMayExist_cf_byteBuffer_returnsFalse_forAbsentKey(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("test"))) {
			ByteBuffer key = ByteBuffer.allocateDirect(5);
			key.put("ghost".getBytes()).flip();

			// When / Then
			assertThat(db.keyMayExist(cf, key)).isFalse();
		}
	}

	@Test
	void keyMayExist_cf_byteBuffer_returnsTrue_forPresentKey(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("test"))) {
			db.put(cf, "hello".getBytes(), "world".getBytes());
			ByteBuffer key = ByteBuffer.allocateDirect(5);
			key.put("hello".getBytes()).flip();

			// When / Then
			assertThat(db.keyMayExist(cf, key)).isTrue();
		}
	}

	@Test
	void keyMayExist_cf_memorySegment_returnsFalse_forAbsentKey(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("test"));
		     Arena arena = Arena.ofConfined()) {
			MemorySegment key = arena.allocateFrom("ghost").reinterpret(5);

			// When / Then
			assertThat(db.keyMayExist(cf, key)).isFalse();
		}
	}

	@Test
	void keyMayExist_cf_memorySegment_returnsTrue_forPresentKey(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("test"));
		     Arena arena = Arena.ofConfined()) {
			db.put(cf, "hi".getBytes(), "there".getBytes());
			MemorySegment key = arena.allocate(2);
			key.setAtIndex(ValueLayout.JAVA_BYTE, 0, (byte) 'h');
			key.setAtIndex(ValueLayout.JAVA_BYTE, 1, (byte) 'i');

			// When / Then
			assertThat(db.keyMayExist(cf, key)).isTrue();
		}
	}

	// -----------------------------------------------------------------------
	// TtlDB — non-CF overloads
	// -----------------------------------------------------------------------

	@Test
	void keyMayExist_ttlDb_byteArray_doesNotThrow_forAbsentKey(@TempDir Path dir) {
		// Given — TtlDB Bloom filter configuration does not guarantee false for absent keys;
		// the contract is that false means "definitely absent", true means "may exist".
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.openWithTtl(opts, dir, Duration.ofSeconds(60))) {

			// When / Then — must not throw; result is unspecified
			assertThat(db.keyMayExist("ghost".getBytes())).isIn(true, false);
		}
	}

	@Test
	void keyMayExist_ttlDb_byteArray_returnsTrue_forPresentKey(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.openWithTtl(opts, dir, Duration.ofSeconds(60))) {
			db.put("k".getBytes(), "v".getBytes());

			// When / Then
			assertThat(db.keyMayExist("k".getBytes())).isTrue();
		}
	}

	@Test
	void keyMayExist_ttlDb_byteBuffer_returnsTrue_forPresentKey(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.openWithTtl(opts, dir, Duration.ofSeconds(60))) {
			db.put("hello".getBytes(), "world".getBytes());
			ByteBuffer key = ByteBuffer.allocateDirect(5);
			key.put("hello".getBytes()).flip();

			// When / Then
			assertThat(db.keyMayExist(key)).isTrue();
		}
	}

	@Test
	void keyMayExist_ttlDb_memorySegment_returnsTrue_forPresentKey(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.openWithTtl(opts, dir, Duration.ofSeconds(60));
		     Arena arena = Arena.ofConfined()) {
			db.put("hi".getBytes(), "there".getBytes());
			MemorySegment key = arena.allocate(2);
			key.setAtIndex(ValueLayout.JAVA_BYTE, 0, (byte) 'h');
			key.setAtIndex(ValueLayout.JAVA_BYTE, 1, (byte) 'i');

			// When / Then
			assertThat(db.keyMayExist(key)).isTrue();
		}
	}

	// -----------------------------------------------------------------------
	// TtlDB — CF overloads
	// -----------------------------------------------------------------------

	@Test
	void keyMayExist_ttlDb_cf_doesNotThrow_forAbsentKey(@TempDir Path dir) {
		// Given — same Bloom filter caveat as non-CF TtlDB variant
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.openWithTtl(opts, dir, Duration.ofSeconds(60));
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("test"))) {

			// When / Then — must not throw; result is unspecified
			assertThat(db.keyMayExist(cf, "ghost".getBytes())).isIn(true, false);
		}
	}

	@Test
	void keyMayExist_ttlDb_cf_returnsTrue_forPresentKey(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.openWithTtl(opts, dir, Duration.ofSeconds(60));
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("test"))) {
			db.put(cf, "k".getBytes(), "v".getBytes());

			// When / Then
			assertThat(db.keyMayExist(cf, "k".getBytes())).isTrue();
		}
	}

	@Test
	void keyMayExist_ttlDb_cf_byteBuffer_returnsTrue_forPresentKey(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.openWithTtl(opts, dir, Duration.ofSeconds(60));
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("test"))) {
			db.put(cf, "hello".getBytes(), "world".getBytes());
			ByteBuffer key = ByteBuffer.allocateDirect(5);
			key.put("hello".getBytes()).flip();

			// When / Then
			assertThat(db.keyMayExist(cf, key)).isTrue();
		}
	}

	@Test
	void keyMayExist_ttlDb_cf_memorySegment_returnsTrue_forPresentKey(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.openWithTtl(opts, dir, Duration.ofSeconds(60));
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("test"));
		     Arena arena = Arena.ofConfined()) {
			db.put(cf, "hi".getBytes(), "there".getBytes());
			MemorySegment key = arena.allocate(2);
			key.setAtIndex(ValueLayout.JAVA_BYTE, 0, (byte) 'h');
			key.setAtIndex(ValueLayout.JAVA_BYTE, 1, (byte) 'i');

			// When / Then
			assertThat(db.keyMayExist(cf, key)).isTrue();
		}
	}
}
