package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SstFileWriterTest {

	// -----------------------------------------------------------------------
	// SstFileWriter — basic write and ingest
	// -----------------------------------------------------------------------

	@Test
	void ingest_singleFile_keysAreReadable(@TempDir Path dir) {
		// Given
		Path sstPath = dir.resolve("data.sst");
		Path dbPath = dir.resolve("db");

		try (var opts = new Options().setCreateIfMissing(true);
		     var writer = new SstFileWriter(opts)) {
			writer.open(sstPath);
			writer.put("aaa".getBytes(), "val1".getBytes());
			writer.put("bbb".getBytes(), "val2".getBytes());
			writer.put("ccc".getBytes(), "val3".getBytes());
			writer.finish();
		}

		// When
		try (var db = RocksDB.open(dbPath)) {
			db.ingestExternalFile(sstPath);

			// Then
			assertThat(db.get("aaa".getBytes())).isEqualTo("val1".getBytes());
			assertThat(db.get("bbb".getBytes())).isEqualTo("val2".getBytes());
			assertThat(db.get("ccc".getBytes())).isEqualTo("val3".getBytes());
		}
	}

	@Test
	void ingest_multipleFiles_allKeysReadable(@TempDir Path dir) {
		// Given — two non-overlapping SST files
		Path sst1 = dir.resolve("file1.sst");
		Path sst2 = dir.resolve("file2.sst");
		Path dbPath = dir.resolve("db");

		try (var opts = new Options().setCreateIfMissing(true);
		     var writer = new SstFileWriter(opts)) {
			writer.open(sst1);
			writer.put("aaa".getBytes(), "v1".getBytes());
			writer.put("bbb".getBytes(), "v2".getBytes());
			writer.finish();

			writer.open(sst2);
			writer.put("ccc".getBytes(), "v3".getBytes());
			writer.put("ddd".getBytes(), "v4".getBytes());
			writer.finish();
		}

		// When
		try (var db = RocksDB.open(dbPath)) {
			db.ingestExternalFile(List.of(sst1, sst2));

			// Then
			assertThat(db.get("aaa".getBytes())).isEqualTo("v1".getBytes());
			assertThat(db.get("bbb".getBytes())).isEqualTo("v2".getBytes());
			assertThat(db.get("ccc".getBytes())).isEqualTo("v3".getBytes());
			assertThat(db.get("ddd".getBytes())).isEqualTo("v4".getBytes());
		}
	}

	@Test
	void ingest_withExplicitOptions_moveFiles(@TempDir Path dir) {
		// Given
		Path sstPath = dir.resolve("data.sst");
		Path dbPath = dir.resolve("db");

		try (var opts = new Options().setCreateIfMissing(true);
		     var writer = new SstFileWriter(opts)) {
			writer.open(sstPath);
			writer.put("key".getBytes(), "value".getBytes());
			writer.finish();
		}

		// When
		try (var db = RocksDB.open(dbPath);
		     var ingestOpts = new IngestExternalFileOptions().setMoveFiles(true)) {
			db.ingestExternalFile(sstPath, ingestOpts);

			// Then
			assertThat(db.get("key".getBytes())).isEqualTo("value".getBytes());
		}
	}

	@Test
	void ingest_doesNotAffectExistingKeys(@TempDir Path dir) {
		// Given — DB has an existing key; SST has a different key
		Path sstPath = dir.resolve("data.sst");
		Path dbPath = dir.resolve("db");

		try (var opts = new Options().setCreateIfMissing(true);
		     var writer = new SstFileWriter(opts)) {
			writer.open(sstPath);
			writer.put("sst-key".getBytes(), "sst-val".getBytes());
			writer.finish();
		}

		// When
		try (var db = RocksDB.open(dbPath)) {
			db.put("existing".getBytes(), "original".getBytes());
			db.ingestExternalFile(sstPath);

			// Then
			assertThat(db.get("existing".getBytes())).isEqualTo("original".getBytes());
			assertThat(db.get("sst-key".getBytes())).isEqualTo("sst-val".getBytes());
		}
	}

	@Test
	void fileSize_returnsPositiveSizeAfterFinish(@TempDir Path dir) {
		// Given
		Path sstPath = dir.resolve("data.sst");

		try (var opts = new Options().setCreateIfMissing(true);
		     var writer = new SstFileWriter(opts)) {
			// When
			writer.open(sstPath);
			writer.put("key1".getBytes(), "value1".getBytes());
			writer.put("key2".getBytes(), "value2".getBytes());
			writer.finish();

			// Then
			assertThat(writer.fileSize()).isPositive();
		}
	}

	// -----------------------------------------------------------------------
	// IngestExternalFileOptions — option round-trips
	// -----------------------------------------------------------------------

	@Test
	void ingestExternalFileOptions_setMoveFiles_doesNotThrow() {
		// Given / When / Then
		try (var opts = new IngestExternalFileOptions()) {
			opts.setMoveFiles(true);
			opts.setMoveFiles(false);
		}
	}

	@Test
	void ingestExternalFileOptions_allSetters_chaining() {
		// Given / When / Then — verify fluent API compiles and does not throw
		try (var opts = new IngestExternalFileOptions()
				.setMoveFiles(false)
				.setSnapshotConsistency(true)
				.setAllowGlobalSeqno(true)
				.setAllowBlockingFlush(true)
				.setIngestBehind(false)
				.setFailIfNotBottommostLevel(false)) {
			assertThat(opts).isNotNull();
		}
	}

	// -----------------------------------------------------------------------
	// Error handling
	// -----------------------------------------------------------------------

	@Test
	void ingest_emptyFileList_isNoOp(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			// When / Then — empty list should not throw
			db.ingestExternalFile(List.of());
		}
	}

	@Test
	void open_nonExistentDirectory_throws(@TempDir Path dir) {
		// Given
		Path sstPath = dir.resolve("nonexistent").resolve("data.sst");

		try (var opts = new Options().setCreateIfMissing(true);
		     var writer = new SstFileWriter(opts)) {
			// When / Then
			assertThatThrownBy(() -> writer.open(sstPath))
					.isInstanceOf(RocksDBException.class);
		}
	}

	@Test
	void put_outOfOrder_throws(@TempDir Path dir) {
		// Given — keys must be in ascending order
		Path sstPath = dir.resolve("data.sst");

		try (var opts = new Options().setCreateIfMissing(true);
		     var writer = new SstFileWriter(opts)) {
			writer.open(sstPath);
			writer.put("zzz".getBytes(), "v1".getBytes());

			// When / Then — adding a key that sorts before the previous one must fail
			assertThatThrownBy(() -> writer.put("aaa".getBytes(), "v2".getBytes()))
					.isInstanceOf(RocksDBException.class);
		}
	}
}
