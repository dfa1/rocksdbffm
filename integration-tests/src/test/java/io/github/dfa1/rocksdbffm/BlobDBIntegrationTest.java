package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class BlobDBIntegrationTest {

	/// 64 KB value — large enough to be stored in a blob file regardless of the min-blob-size setting.
	private static final byte[] VALUE_64KB = new byte[(int) MemorySize.ofKB(64).toBytes()];

	@Test
	void putGet_withBlobFiles(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions()
				.setCreateIfMissing(true)
				.setEnableBlobFiles(true)
				.setMinBlobSize(MemorySize.ofBytes(0));
		     var db = RocksDB.openWithBlobFiles(opts, dir)) {

			// When
			db.put("k".getBytes(), VALUE_64KB);

			// Then
			assertThat(db.get("k".getBytes())).isEqualTo(VALUE_64KB);
		}
	}

	@Test
	void openWithBlobFiles_convenienceFactory(@TempDir Path dir) {
		// Given — convenience overload sets createIfMissing=true and enableBlobFiles=true
		try (var db = RocksDB.openWithBlobFiles(dir)) {

			// When
			db.put("hello".getBytes(), VALUE_64KB);

			// Then
			assertThat(db.get("hello".getBytes())).isEqualTo(VALUE_64KB);
		}
	}

	@Test
	void delete_removesKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openWithBlobFiles(dir)) {
			db.put("k".getBytes(), VALUE_64KB);

			// When
			db.delete("k".getBytes());

			// Then
			assertThat(db.get("k".getBytes())).isNull();
		}
	}

	@Test
	void blobOptions_roundTrip(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions()
				.setCreateIfMissing(true)
				.setEnableBlobFiles(true)
				.setMinBlobSize(MemorySize.ofKB(4))
				.setBlobFileSize(MemorySize.ofMB(64))
				.setBlobCompressionType(CompressionType.NO_COMPRESSION)
				.setEnableBlobGc(true)
				.setBlobGcAgeCutoff(0.25)
				.setBlobGcForceThreshold(0.8)
				.setBlobFileStartingLevel(0)) {

			// When
			var enableBlobFiles = opts.getEnableBlobFiles();
			var minBlobSize = opts.getMinBlobSize();
			var blobFileSize = opts.getBlobFileSize();
			var blobCompressionType = opts.getBlobCompressionType();
			var enableBlobGc = opts.getEnableBlobGc();
			var blobGcAgeCutoff = opts.getBlobGcAgeCutoff();
			var blobGcForceThreshold = opts.getBlobGcForceThreshold();
			var blobFileStartingLevel = opts.getBlobFileStartingLevel();

			// Then — options are readable back
			assertThat(enableBlobFiles).isTrue();
			assertThat(minBlobSize).isEqualTo(MemorySize.ofKB(4));
			assertThat(blobFileSize).isEqualTo(MemorySize.ofMB(64));
			assertThat(blobCompressionType).isEqualTo(CompressionType.NO_COMPRESSION);
			assertThat(enableBlobGc).isTrue();
			assertThat(blobGcAgeCutoff).isEqualTo(0.25);
			assertThat(blobGcForceThreshold).isEqualTo(0.8);
			assertThat(blobFileStartingLevel).isEqualTo(0);

			try (var db = RocksDB.openWithBlobFiles(opts, dir)) {
				db.put("k".getBytes(), VALUE_64KB);
				assertThat(db.get("k".getBytes())).isEqualTo(VALUE_64KB);
			}
		}
	}

	@Test
	void checkpoint_capturesSnapshot(@TempDir Path dir) {
		// Given — minBlobSize larger than value so "snap" stays in SST (not a blob file),
		// but blob files are still enabled. This exercises Checkpoint.newCheckpoint(BlobDB).
		var dbDir = dir.resolve("db");
		var checkpointDir = dir.resolve("checkpoint");
		try (var opts = Options.newOptions()
				.setCreateIfMissing(true)
				.setEnableBlobFiles(true)
				.setMinBlobSize(MemorySize.ofKB(64));
		     var db = RocksDB.openWithBlobFiles(opts, dbDir)) {
			db.put("before".getBytes(), "snap".getBytes());

			// When — take a checkpoint
			try (var cp = Checkpoint.newCheckpoint(db)) {
				cp.exportTo(checkpointDir);
			}

			db.put("after".getBytes(), "late".getBytes());

			// Then — checkpoint contains only the pre-checkpoint key
			try (var snap = RocksDB.openReadOnly(checkpointDir)) {
				assertThat(snap.get("before".getBytes())).isEqualTo("snap".getBytes());
				assertThat(snap.get("after".getBytes())).isNull();
			}
		}
	}

	@Test
	void numBlobFiles_property_isAvailable(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions()
				.setCreateIfMissing(true)
				.setEnableBlobFiles(true)
				.setMinBlobSize(MemorySize.ofBytes(0));
		     var db = RocksDB.openWithBlobFiles(opts, dir)) {

			db.put("k".getBytes(), VALUE_64KB);
			db.flush(FlushOptions.newFlushOptions());

			// When
			var numBlobFiles = db.getLongProperty(Property.NUM_BLOB_FILES);

			// Then — blob file count property is readable (value ≥ 0)
			assertThat(numBlobFiles).isPresent();
			assertThat(numBlobFiles.getAsLong()).isGreaterThanOrEqualTo(1);
		}
	}
}
