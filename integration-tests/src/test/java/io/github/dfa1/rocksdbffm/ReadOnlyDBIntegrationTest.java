package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReadOnlyDBIntegrationTest {

	@Test
	void readsDataWrittenByPrimary(@TempDir Path dir) {
		// Given — write data with a read-write db, then close it
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {
			db.put("k".getBytes(), "v".getBytes());
		}

		// When
		try (var db = RocksDB.openReadOnly(dir)) {

			// Then
			assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
			assertThat(db.get("missing".getBytes())).isNull();
		}
	}

	@Test
	void iterator_scansAllKeys(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {
			db.put("a".getBytes(), "1".getBytes());
			db.put("b".getBytes(), "2".getBytes());
			db.put("c".getBytes(), "3".getBytes());
		}

		// When
		try (var db = RocksDB.openReadOnly(dir)) {
			List<String> keys = new ArrayList<>();
			try (var it = db.newIterator()) {
				for (it.seekToFirst(); it.isValid(); it.next()) {
					keys.add(new String(it.key()));
				}
			}

			// Then
			assertThat(keys).containsExactly("a", "b", "c");
		}
	}


}
