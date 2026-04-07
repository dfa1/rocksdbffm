package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that passing null to any public API method raises a Java exception
 * rather than crashing the JVM via a native SIGSEGV.
 * <p>
 * The test suite completing successfully IS the proof: a JVM crash would abort
 * the test process entirely rather than reporting a failure.
 */
class NullSafetyTest {

	// -----------------------------------------------------------------------
	// RocksDB.open / openReadOnly — static factories
	// -----------------------------------------------------------------------

	@Test
	void open_nullOptions(@TempDir Path dir) {
		assertThatThrownBy(() -> RocksDB.open(null, dir))
				.isInstanceOf(RuntimeException.class);
	}

	@Test
	void open_nullPath() {
		try (var opts = new Options()) {
			assertThatThrownBy(() -> RocksDB.open(opts, null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void openReadOnly_nullOptions(@TempDir Path dir) {
		assertThatThrownBy(() -> RocksDB.openReadOnly(null, dir))
				.isInstanceOf(RuntimeException.class);
	}

	@Test
	void openReadOnly_nullPath() {
		try (var opts = new Options()) {
			assertThatThrownBy(() -> RocksDB.openReadOnly(opts, null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	// -----------------------------------------------------------------------
	// RocksDB — byte[] variants
	// -----------------------------------------------------------------------

	@Test
	void put_nullKey(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			assertThatThrownBy(() -> db.put(null, "v".getBytes()))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void put_nullValue(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			assertThatThrownBy(() -> db.put("k".getBytes(), null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void get_nullKey(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			assertThatThrownBy(() -> db.get((byte[]) null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void get_nullReadOptions(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			assertThatThrownBy(() -> db.get(null, "k".getBytes()))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void get_nullKeyWithReadOptions(@TempDir Path dir) {
		try (var db = RocksDB.open(dir);
		     var ro = new ReadOptions()) {
			assertThatThrownBy(() -> db.get(ro, null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void delete_nullKey(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			assertThatThrownBy(() -> db.delete(null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	// -----------------------------------------------------------------------
	// RocksDB — ByteBuffer variants
	// -----------------------------------------------------------------------

	@Test
	void put_nullKeyBuffer(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			var value = ByteBuffer.allocateDirect(4).put("v".getBytes()).flip();
			assertThatThrownBy(() -> db.put((ByteBuffer) null, value))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void put_nullValueBuffer(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			var key = ByteBuffer.allocateDirect(4).put("k".getBytes()).flip();
			assertThatThrownBy(() -> db.put(key, (ByteBuffer) null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void get_nullKeyBuffer(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			var value = ByteBuffer.allocateDirect(128);
			assertThatThrownBy(() -> db.get((ByteBuffer) null, value))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void get_nullValueBuffer(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), "v".getBytes());
			var key = ByteBuffer.allocateDirect(4).put("k".getBytes()).flip();
			assertThatThrownBy(() -> db.get(key, (ByteBuffer) null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	// -----------------------------------------------------------------------
	// RocksDB — keyMayExist
	// -----------------------------------------------------------------------

	@Test
	void keyMayExist_nullByteArray(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			assertThatThrownBy(() -> db.keyMayExist((byte[]) null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void keyMayExist_nullReadOptions(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			assertThatThrownBy(() -> db.keyMayExist(null, "k".getBytes()))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void keyMayExist_nullKeyWithReadOptions(@TempDir Path dir) {
		try (var db = RocksDB.open(dir);
		     var ro = new ReadOptions()) {
			assertThatThrownBy(() -> db.keyMayExist(ro, null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void keyMayExist_nullByteBuffer(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			assertThatThrownBy(() -> db.keyMayExist((ByteBuffer) null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void keyMayExist_nullMemorySegment(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			assertThatThrownBy(() -> db.keyMayExist((MemorySegment) null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	// -----------------------------------------------------------------------
	// RocksDB — iterator, flush, write, properties
	// -----------------------------------------------------------------------

	@Test
	void newIterator_nullReadOptions(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			assertThatThrownBy(() -> db.newIterator(null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void flush_nullFlushOptions(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			assertThatThrownBy(() -> db.flush(null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void write_nullBatch(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			assertThatThrownBy(() -> db.write(null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void getProperty_null(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			assertThatThrownBy(() -> db.getProperty(null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void getLongProperty_null(@TempDir Path dir) {
		try (var db = RocksDB.open(dir)) {
			assertThatThrownBy(() -> db.getLongProperty(null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	// -----------------------------------------------------------------------
	// WriteBatch
	// -----------------------------------------------------------------------

	@Test
	void writeBatch_put_nullKey() {
		try (var batch = WriteBatch.create()) {
			assertThatThrownBy(() -> batch.put(null, "v".getBytes()))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void writeBatch_put_nullValue() {
		try (var batch = WriteBatch.create()) {
			assertThatThrownBy(() -> batch.put("k".getBytes(), null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void writeBatch_delete_nullKey() {
		try (var batch = WriteBatch.create()) {
			assertThatThrownBy(() -> batch.delete(null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	// -----------------------------------------------------------------------
	// RocksIterator — seek / seekForPrev
	// -----------------------------------------------------------------------

	@Test
	void iterator_seek_nullByteArray(@TempDir Path dir) {
		try (var db = RocksDB.open(dir);
		     var it = db.newIterator()) {
			assertThatThrownBy(() -> it.seek((byte[]) null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void iterator_seek_nullByteBuffer(@TempDir Path dir) {
		try (var db = RocksDB.open(dir);
		     var it = db.newIterator()) {
			assertThatThrownBy(() -> it.seek((ByteBuffer) null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void iterator_seek_nullMemorySegment(@TempDir Path dir) {
		try (var db = RocksDB.open(dir);
		     var it = db.newIterator()) {
			assertThatThrownBy(() -> it.seek((MemorySegment) null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void iterator_seekForPrev_nullByteArray(@TempDir Path dir) {
		try (var db = RocksDB.open(dir);
		     var it = db.newIterator()) {
			assertThatThrownBy(() -> it.seekForPrev((byte[]) null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void iterator_seekForPrev_nullByteBuffer(@TempDir Path dir) {
		try (var db = RocksDB.open(dir);
		     var it = db.newIterator()) {
			assertThatThrownBy(() -> it.seekForPrev((ByteBuffer) null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void iterator_seekForPrev_nullMemorySegment(@TempDir Path dir) {
		try (var db = RocksDB.open(dir);
		     var it = db.newIterator()) {
			assertThatThrownBy(() -> it.seekForPrev((MemorySegment) null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	// -----------------------------------------------------------------------
	// RocksIterator — key/value buffer copy
	// -----------------------------------------------------------------------

	@Test
	void iterator_key_nullBuffer(@TempDir Path dir) {
		try (var db = RocksDB.open(dir);
		     var it = db.newIterator()) {
			db.put("k".getBytes(), "v".getBytes());
			it.seekToFirst();
			assertThatThrownBy(() -> it.key((ByteBuffer) null))
					.isInstanceOf(RuntimeException.class);
		}
	}

	@Test
	void iterator_value_nullBuffer(@TempDir Path dir) {
		try (var db = RocksDB.open(dir);
		     var it = db.newIterator()) {
			db.put("k".getBytes(), "v".getBytes());
			it.seekToFirst();
			assertThatThrownBy(() -> it.value((ByteBuffer) null))
					.isInstanceOf(RuntimeException.class);
		}
	}
}
