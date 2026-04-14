package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ColumnFamilyTest {

	// -----------------------------------------------------------------------
	// listColumnFamilies
	// -----------------------------------------------------------------------

	@Test
	void listColumnFamilies_returnsDefaultOnly_onNewDb(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {
			// When
			List<byte[]> families = RocksDB.listColumnFamilies(opts, dir);

			// Then
			assertThat(families).hasSize(1);
			assertThat(new String(families.get(0), StandardCharsets.UTF_8)).isEqualTo("default");
		}
	}

	// -----------------------------------------------------------------------
	// createColumnFamily / dropColumnFamily
	// -----------------------------------------------------------------------

	@Test
	void createColumnFamily_canPutAndGetInNewFamily(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"))) {

			// When
			db.put(cf, "key".getBytes(), "value".getBytes());

			// Then
			assertThat(db.get(cf, "key".getBytes())).isEqualTo("value".getBytes());
		}
	}

	@Test
	void createColumnFamily_isolatedFromDefaultFamily(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"))) {
			db.put("key".getBytes(), "default-value".getBytes());
			db.put(cf, "key".getBytes(), "cf-value".getBytes());

			// When
			var defaultResult = db.get("key".getBytes());
			var cfResult = db.get(cf, "key".getBytes());

			// Then
			assertThat(defaultResult).isEqualTo("default-value".getBytes());
			assertThat(cfResult).isEqualTo("cf-value".getBytes());
		}
	}

	@Test
	void dropColumnFamily_removesFamily(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("to-drop"));
			db.put(cf, "key".getBytes(), "value".getBytes());

			// When
			db.dropColumnFamily(cf);
			cf.close();

			// Then — family list should only contain default
			try (var opts = Options.newOptions()) {
				List<byte[]> families = RocksDB.listColumnFamilies(opts, dir);
				List<String> names = families.stream()
						.map(b -> new String(b, StandardCharsets.UTF_8))
						.toList();
				assertThat(names).containsExactly("default");
			}
		}
	}

	// -----------------------------------------------------------------------
	// openWithColumnFamilies
	// -----------------------------------------------------------------------

	@Test
	void openWithColumnFamilies_persistsAndReadsAcrossReopens(@TempDir Path dir) {
		// Given — create a DB with a non-default CF and write data
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("users"))) {
			db.put(cf, "alice".getBytes(), "data".getBytes());
		}

		// When — reopen with both CFs
		List<ColumnFamilyHandle> handles = new ArrayList<>();
		try (var opts = Options.newOptions().setCreateIfMissing(false);
		     var db = RocksDB.openWithColumnFamilies(opts, dir,
				     List.of(ColumnFamilyDescriptor.of("default"),
						     ColumnFamilyDescriptor.of("users")),
				     handles)) {
			var defaultCf = handles.get(0);
			var usersCf = handles.get(1);

			// Then
			assertThat(db.get(usersCf, "alice".getBytes())).isEqualTo("data".getBytes());
			assertThat(db.get(defaultCf, "alice".getBytes())).isNull();

			handles.forEach(ColumnFamilyHandle::close);
		}
	}

	// -----------------------------------------------------------------------
	// ColumnFamilyHandle metadata
	// -----------------------------------------------------------------------

	@Test
	void columnFamilyHandle_returnsCorrectName(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("my-cf"))) {

			// When
			String name = cf.getName();

			// Then
			assertThat(name).isEqualTo("my-cf");
		}
	}

	@Test
	void columnFamilyHandle_defaultIdIsZero(@TempDir Path dir) {
		// Given
		List<ColumnFamilyHandle> handles = new ArrayList<>();
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.openWithColumnFamilies(opts, dir,
				     List.of(ColumnFamilyDescriptor.of("default")),
				     handles)) {

			// When
			int id = handles.get(0).getId();

			// Then
			assertThat(id).isEqualTo(0);
			handles.forEach(ColumnFamilyHandle::close);
		}
	}

	// -----------------------------------------------------------------------
	// delete — CF overloads
	// -----------------------------------------------------------------------

	@Test
	void delete_removesKeyFromColumnFamily(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"))) {
			db.put(cf, "k".getBytes(), "v".getBytes());

			// When
			db.delete(cf, "k".getBytes());

			// Then
			assertThat(db.get(cf, "k".getBytes())).isNull();
		}
	}

	// -----------------------------------------------------------------------
	// deleteRange — CF overloads
	// -----------------------------------------------------------------------

	@Test
	void deleteRange_removesKeyRangeFromColumnFamily(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"))) {
			db.put(cf, "a".getBytes(), "1".getBytes());
			db.put(cf, "b".getBytes(), "2".getBytes());
			db.put(cf, "c".getBytes(), "3".getBytes());

			// When
			db.deleteRange(cf, "a".getBytes(), "c".getBytes());

			// Then
			assertThat(db.get(cf, "a".getBytes())).isNull();
			assertThat(db.get(cf, "b".getBytes())).isNull();
			assertThat(db.get(cf, "c".getBytes())).isEqualTo("3".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// keyMayExist — CF overloads
	// -----------------------------------------------------------------------

	@Test
	void keyMayExist_returnsTrueForPresentKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"))) {
			db.put(cf, "k".getBytes(), "v".getBytes());

			// When
			var result = db.keyMayExist(cf, "k".getBytes());

			// Then
			assertThat(result).isTrue();
		}
	}

	@Test
	void keyMayExist_returnsFalseForAbsentKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"))) {

			// When
			var result = db.keyMayExist(cf, "absent".getBytes());

			// Then
			assertThat(result).isFalse();
		}
	}

	// -----------------------------------------------------------------------
	// ByteBuffer / MemorySegment overloads
	// -----------------------------------------------------------------------

	@Test
	void put_get_byteBuffer_overload(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"))) {
			ByteBuffer key = ByteBuffer.allocateDirect(1).put((byte) 'k').flip();
			ByteBuffer val = ByteBuffer.allocateDirect(1).put((byte) 'v').flip();

			// When
			db.put(cf, key, val);

			ByteBuffer getKey = ByteBuffer.allocateDirect(1).put((byte) 'k').flip();
			ByteBuffer getVal = ByteBuffer.allocateDirect(64);

			// Then
			assertThat(db.get(cf, getKey, getVal)).isEqualTo(1);
		}
	}

	// -----------------------------------------------------------------------
	// Iterator — CF overloads
	// -----------------------------------------------------------------------

	@Test
	void newIterator_scansKeysInColumnFamily(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"))) {
			db.put(cf, "a".getBytes(), "1".getBytes());
			db.put(cf, "b".getBytes(), "2".getBytes());

			// When
			List<String> keys = new ArrayList<>();
			try (var it = db.newIterator(cf)) {
				for (it.seekToFirst(); it.isValid(); it.next()) {
					keys.add(new String(it.key(), StandardCharsets.UTF_8));
				}
				it.checkError();
			}

			// Then
			assertThat(keys).containsExactly("a", "b");
		}
	}

	@Test
	void newIterator_doesNotSeeKeysFromOtherFamily(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"))) {
			db.put("default-key".getBytes(), "x".getBytes());
			db.put(cf, "cf-key".getBytes(), "y".getBytes());

			// When — iterate over cf1
			List<String> keys = new ArrayList<>();
			try (var it = db.newIterator(cf)) {
				for (it.seekToFirst(); it.isValid(); it.next()) {
					keys.add(new String(it.key(), StandardCharsets.UTF_8));
				}
			}

			// Then — only cf1 keys visible
			assertThat(keys).containsExactly("cf-key");
		}
	}

	// -----------------------------------------------------------------------
	// WriteBatch — CF overloads
	// -----------------------------------------------------------------------

	@Test
	void writeBatch_putAndDelete_inColumnFamily(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"));
		     var batch = WriteBatch.create()) {

			// When
			batch.put(cf, "k1".getBytes(), "v1".getBytes());
			batch.put(cf, "k2".getBytes(), "v2".getBytes());
			batch.delete(cf, "k1".getBytes());
			db.write(batch);

			// Then
			assertThat(db.get(cf, "k1".getBytes())).isNull();
			assertThat(db.get(cf, "k2".getBytes())).isEqualTo("v2".getBytes());
		}
	}

	@Test
	void writeBatch_deleteRange_inColumnFamily(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"));
		     var batch = WriteBatch.create()) {
			db.put(cf, "a".getBytes(), "1".getBytes());
			db.put(cf, "b".getBytes(), "2".getBytes());
			db.put(cf, "c".getBytes(), "3".getBytes());

			// When
			batch.deleteRange(cf, "a".getBytes(), "c".getBytes());
			db.write(batch);

			// Then
			assertThat(db.get(cf, "a".getBytes())).isNull();
			assertThat(db.get(cf, "b".getBytes())).isNull();
			assertThat(db.get(cf, "c".getBytes())).isEqualTo("3".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// flush / getProperty — CF overloads
	// -----------------------------------------------------------------------

	@Test
	void flush_columnFamily(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"));
		     var flushOpts = FlushOptions.newFlushOptions().setWait(true)) {
			db.put(cf, "k".getBytes(), "v".getBytes());

			// When
			db.flush(cf, flushOpts);

			// Then — no exception means flush succeeded
		}
	}

	@Test
	void getProperty_columnFamily(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"))) {

			// When
			var prop = db.getProperty(cf, Property.NUM_ENTRIES_ACTIVE_MEM_TABLE);

			// Then
			assertThat(prop).isPresent();
		}
	}
}
