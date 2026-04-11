package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReadWriteDBIntegrationTest {

	@Test
	void putGetDelete(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {

			// When
			db.put("k1".getBytes(), "v1".getBytes());
			db.put("k2".getBytes(), "v2".getBytes());

			// Then
			assertThat(db.get("k1".getBytes())).isEqualTo("v1".getBytes());
			assertThat(db.get("k2".getBytes())).isEqualTo("v2".getBytes());

			// When
			db.delete("k1".getBytes());

			// Then
			assertThat(db.get("k1".getBytes())).isNull();
			assertThat(db.get("k2".getBytes())).isEqualTo("v2".getBytes());
		}
	}

	@Test
	void writeBatch_appliesAtomically(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var batch = WriteBatch.create()) {

			// When
			batch.put("k1".getBytes(), "v1".getBytes());
			batch.put("k2".getBytes(), "v2".getBytes());
			batch.delete("k1".getBytes());
			db.write(batch);

			// Then
			assertThat(db.get("k1".getBytes())).isNull();
			assertThat(db.get("k2".getBytes())).isEqualTo("v2".getBytes());
		}
	}

	@Test
	void iterator_scansKeysInOrder(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {

			db.put("a".getBytes(), "1".getBytes());
			db.put("c".getBytes(), "3".getBytes());
			db.put("b".getBytes(), "2".getBytes());

			// When
			List<String> keys = new ArrayList<>();
			try (var it = db.newIterator()) {
				for (it.seekToFirst(); it.isValid(); it.next()) {
					keys.add(new String(it.key()));
				}
			}

			// Then — RocksDB stores keys in sorted order
			assertThat(keys).containsExactly("a", "b", "c");
		}
	}
}
