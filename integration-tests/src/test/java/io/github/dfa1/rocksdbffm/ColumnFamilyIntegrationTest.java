package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end column family tests that exercise persistence across DB close/reopen cycles and
/// cross-feature interactions (snapshots, WriteBatch, iterators, flush).
class ColumnFamilyIntegrationTest {

	// -----------------------------------------------------------------------
	// Persistence across reopen
	// -----------------------------------------------------------------------

	@Test
	void data_survivesCloseAndReopen(@TempDir Path dir) {
		// Given — write to a custom CF, then close the DB
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("accounts"))) {
			db.put(cf, "alice".getBytes(), "100".getBytes());
			db.put(cf, "bob".getBytes(), "200".getBytes());
		}

		// When — reopen with both column families
		List<ColumnFamilyHandle> handles = new ArrayList<>();
		try (var opts = Options.newOptions();
		     var db = RocksDB.openWithColumnFamilies(opts, dir,
				     List.of(ColumnFamilyDescriptor.of("default"),
						     ColumnFamilyDescriptor.of("accounts")),
				     handles)) {
			var accountsCf = handles.get(1);

			// Then — data still present
			assertThat(db.get(accountsCf, "alice".getBytes())).isEqualTo("100".getBytes());
			assertThat(db.get(accountsCf, "bob".getBytes())).isEqualTo("200".getBytes());
			assertThat(db.get(accountsCf, "carol".getBytes())).isNull();

			handles.forEach(ColumnFamilyHandle::close);
		}
	}

	@Test
	void multipleColumnFamilies_persistIndependently(@TempDir Path dir) {
		// Given — create two CFs and write distinct data to each
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var usersCf = db.createColumnFamily(ColumnFamilyDescriptor.of("users"));
		     var ordersCf = db.createColumnFamily(ColumnFamilyDescriptor.of("orders"))) {
			db.put(usersCf, "u1".getBytes(), "alice".getBytes());
			db.put(ordersCf, "o1".getBytes(), "item-a".getBytes());
			db.put("default-key".getBytes(), "default-val".getBytes());
		}

		// When — reopen all three CFs
		List<ColumnFamilyHandle> handles = new ArrayList<>();
		try (var opts = Options.newOptions();
		     var db = RocksDB.openWithColumnFamilies(opts, dir,
				     List.of(ColumnFamilyDescriptor.of("default"),
						     ColumnFamilyDescriptor.of("users"),
						     ColumnFamilyDescriptor.of("orders")),
				     handles)) {
			var defaultCf = handles.get(0);
			var usersCf = handles.get(1);
			var ordersCf = handles.get(2);

			// Then — each CF sees only its own data
			assertThat(db.get(usersCf, "u1".getBytes())).isEqualTo("alice".getBytes());
			assertThat(db.get(ordersCf, "o1".getBytes())).isEqualTo("item-a".getBytes());
			assertThat(db.get(defaultCf, "default-key".getBytes())).isEqualTo("default-val".getBytes());

			assertThat(db.get(usersCf, "o1".getBytes())).isNull();
			assertThat(db.get(ordersCf, "u1".getBytes())).isNull();

			handles.forEach(ColumnFamilyHandle::close);
		}
	}

	// -----------------------------------------------------------------------
	// listColumnFamilies
	// -----------------------------------------------------------------------

	@Test
	void listColumnFamilies_reflectsCreatedAndDroppedFamilies(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {
			var cf1 = db.createColumnFamily(ColumnFamilyDescriptor.of("keep"));
			var cf2 = db.createColumnFamily(ColumnFamilyDescriptor.of("drop-me"));
			db.dropColumnFamily(cf2);
			cf2.close();
			cf1.close();
		}

		// When
		List<String> names;
		try (var opts = Options.newOptions()) {
			names = RocksDB.listColumnFamilies(opts, dir).stream()
					.map(b -> new String(b, StandardCharsets.UTF_8))
					.toList();
		}

		// Then
		assertThat(names).containsExactlyInAnyOrder("default", "keep");
	}

	// -----------------------------------------------------------------------
	// WriteBatch atomicity across CFs
	// -----------------------------------------------------------------------

	@Test
	void writeBatch_appliesAcrossColumnFamiliesAtomically(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var inventoryCf = db.createColumnFamily(ColumnFamilyDescriptor.of("inventory"));
		     var auditCf = db.createColumnFamily(ColumnFamilyDescriptor.of("audit"));
		     var batch = WriteBatch.create()) {

			// When — atomic debit from inventory + audit log entry
			batch.put(inventoryCf, "item-1".getBytes(), "qty:9".getBytes());
			batch.put(auditCf, "tx-001".getBytes(), "sold item-1".getBytes());
			db.write(batch);

			// Then
			assertThat(db.get(inventoryCf, "item-1".getBytes())).isEqualTo("qty:9".getBytes());
			assertThat(db.get(auditCf, "tx-001".getBytes())).isEqualTo("sold item-1".getBytes());
		}
	}

	@Test
	void writeBatch_deleteRange_acrossCfsSurvivesReopen(@TempDir Path dir) {
		// Given — populate a CF and delete a range via WriteBatch
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var logsCf = db.createColumnFamily(ColumnFamilyDescriptor.of("logs"))) {
			for (int i = 1; i <= 5; i++) {
				db.put(logsCf, ("log-" + i).getBytes(), ("entry-" + i).getBytes());
			}
			try (var batch = WriteBatch.create()) {
				batch.deleteRange(logsCf, "log-2".getBytes(), "log-5".getBytes());
				db.write(batch);
			}
		}

		// When — reopen
		List<ColumnFamilyHandle> handles = new ArrayList<>();
		try (var opts = Options.newOptions();
		     var db = RocksDB.openWithColumnFamilies(opts, dir,
				     List.of(ColumnFamilyDescriptor.of("default"),
						     ColumnFamilyDescriptor.of("logs")),
				     handles)) {
			var logsCf = handles.get(1);

			// Then — range [log-2, log-5) is gone, log-1 and log-5 survive
			assertThat(db.get(logsCf, "log-1".getBytes())).isEqualTo("entry-1".getBytes());
			assertThat(db.get(logsCf, "log-2".getBytes())).isNull();
			assertThat(db.get(logsCf, "log-3".getBytes())).isNull();
			assertThat(db.get(logsCf, "log-4".getBytes())).isNull();
			assertThat(db.get(logsCf, "log-5".getBytes())).isEqualTo("entry-5".getBytes());

			handles.forEach(ColumnFamilyHandle::close);
		}
	}

	// -----------------------------------------------------------------------
	// Iterator across reopen
	// -----------------------------------------------------------------------

	@Test
	void iterator_scansPersistedKeysInOrder(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("sorted"))) {
			db.put(cf, "c".getBytes(), "3".getBytes());
			db.put(cf, "a".getBytes(), "1".getBytes());
			db.put(cf, "b".getBytes(), "2".getBytes());
		}

		// When — reopen and iterate
		List<String> keys = new ArrayList<>();
		List<String> values = new ArrayList<>();
		List<ColumnFamilyHandle> handles = new ArrayList<>();
		try (var opts = Options.newOptions();
		     var db = RocksDB.openWithColumnFamilies(opts, dir,
				     List.of(ColumnFamilyDescriptor.of("default"),
						     ColumnFamilyDescriptor.of("sorted")),
				     handles)) {
			try (var it = db.newIterator(handles.get(1))) {
				for (it.seekToFirst(); it.isValid(); it.next()) {
					keys.add(new String(it.key(), StandardCharsets.UTF_8));
					values.add(new String(it.value(), StandardCharsets.UTF_8));
				}
				it.checkError();
			}
			handles.forEach(ColumnFamilyHandle::close);
		}

		// Then — lexicographic order, CF-scoped
		assertThat(keys).containsExactly("a", "b", "c");
		assertThat(values).containsExactly("1", "2", "3");
	}

	// -----------------------------------------------------------------------
	// Snapshot isolation per CF
	// -----------------------------------------------------------------------

	@Test
	void snapshot_seesConsistentViewWithinCf(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("events"))) {
			db.put(cf, "e1".getBytes(), "v1".getBytes());

			// When — take snapshot, then write a new version
			try (var snap = db.getSnapshot();
			     var ro = ReadOptions.newReadOptions().setSnapshot(snap)) {
				db.put(cf, "e1".getBytes(), "v2".getBytes());

				// Then — snapshot still sees old value; direct read sees new one
				assertThat(db.get(cf, ro, "e1".getBytes())).isEqualTo("v1".getBytes());
				assertThat(db.get(cf, "e1".getBytes())).isEqualTo("v2".getBytes());
			}
		}
	}

	// -----------------------------------------------------------------------
	// Flush + property per CF
	// -----------------------------------------------------------------------

	@Test
	void flush_movesDataToSst_reflectedInProperties(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("metrics"));
		     var flushOpts = FlushOptions.newFlushOptions().setWait(true)) {

			for (int i = 0; i < 10; i++) {
				db.put(cf, ("metric-" + i).getBytes(), String.valueOf(i).getBytes());
			}

			// When
			db.flush(cf, flushOpts);

			// Then — memtable should be empty (or reduced) after flush
			var entries = db.getLongProperty(cf, Property.NUM_ENTRIES_ACTIVE_MEM_TABLE);
			assertThat(entries).isPresent();
			assertThat(entries.getAsLong()).isEqualTo(0L);
		}
	}

	// -----------------------------------------------------------------------
	// Column family name / ID metadata
	// -----------------------------------------------------------------------

	@Test
	void columnFamilyHandles_haveUniqueIds(@TempDir Path dir) {
		// Given
		List<ColumnFamilyHandle> handles = new ArrayList<>();
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var cf1 = db.createColumnFamily(ColumnFamilyDescriptor.of("cf1"));
		     var cf2 = db.createColumnFamily(ColumnFamilyDescriptor.of("cf2"))) {

			// When
			int id1 = cf1.getId();
			int id2 = cf2.getId();

			// Then — IDs are distinct and cf1 has a higher ID than default (0)
			assertThat(id1).isNotEqualTo(id2);
			assertThat(id1).isGreaterThan(0);
			assertThat(id2).isGreaterThan(id1);
		}
	}
}
