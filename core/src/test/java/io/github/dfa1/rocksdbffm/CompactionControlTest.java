package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class CompactionControlTest {

	// -----------------------------------------------------------------------
	// compactRange — full keyspace
	// -----------------------------------------------------------------------

	@Test
	void compactRange_noArgs_doesNotThrow(@TempDir Path dir) {
		// Given
		try (var opts = new Options().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {
			db.put("a".getBytes(), "1".getBytes());
			db.put("b".getBytes(), "2".getBytes());

			// When / Then
			db.compactRange();

			assertThat(db.get("a".getBytes())).isEqualTo("1".getBytes());
			assertThat(db.get("b".getBytes())).isEqualTo("2".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// compactRange(byte[], byte[])
	// -----------------------------------------------------------------------

	@Test
	void compactRange_byteArray_fullRange(@TempDir Path dir) {
		// Given
		try (var opts = new Options().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {
			db.put("a".getBytes(), "1".getBytes());
			db.put("z".getBytes(), "2".getBytes());

			// When
			db.compactRange("a".getBytes(), "z".getBytes());

			// Then — data still accessible after compaction
			assertThat(db.get("a".getBytes())).isEqualTo("1".getBytes());
			assertThat(db.get("z".getBytes())).isEqualTo("2".getBytes());
		}
	}

	@Test
	void compactRange_byteArray_nullBounds(@TempDir Path dir) {
		// Given
		try (var opts = new Options().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {
			db.put("k".getBytes(), "v".getBytes());

			// When / Then — null means open-ended
			db.compactRange((byte[]) null, (byte[]) null);

			assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// compactRange(ByteBuffer, ByteBuffer)
	// -----------------------------------------------------------------------

	@Test
	void compactRange_byteBuffer(@TempDir Path dir) {
		// Given — ByteBuffer tier requires direct buffers
		try (var opts = new Options().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {
			db.put("a".getBytes(), "1".getBytes());
			db.put("b".getBytes(), "2".getBytes());

			byte[] startBytes = "a".getBytes();
			byte[] endBytes = "b".getBytes();
			ByteBuffer start = ByteBuffer.allocateDirect(startBytes.length).put(startBytes).flip();
			ByteBuffer end = ByteBuffer.allocateDirect(endBytes.length).put(endBytes).flip();

			// When / Then
			db.compactRange(start, end);

			assertThat(db.get("a".getBytes())).isEqualTo("1".getBytes());
		}
	}

	@Test
	void compactRange_byteBuffer_nullBounds(@TempDir Path dir) {
		// Given
		try (var opts = new Options().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {
			db.put("k".getBytes(), "v".getBytes());

			// When / Then
			db.compactRange((ByteBuffer) null, (ByteBuffer) null);

			assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// compactRange(CompactOptions, byte[], byte[])
	// -----------------------------------------------------------------------

	@Test
	void compactRange_withOptions_fullRange(@TempDir Path dir) {
		// Given
		try (var dbOpts = new Options().setCreateIfMissing(true);
		     var db = RocksDB.open(dbOpts, dir);
		     var compact = new CompactOptions()
					 .setExclusiveManualCompaction(false)
					 .setBottommostLevelCompaction(true)) {

			db.put("a".getBytes(), "1".getBytes());
			db.put("z".getBytes(), "2".getBytes());

			// When
			db.compactRange(compact, null, null);

			// Then
			assertThat(db.get("a".getBytes())).isEqualTo("1".getBytes());
			assertThat(db.get("z".getBytes())).isEqualTo("2".getBytes());
		}
	}

	@Test
	void compactRange_withOptions_changeLevel(@TempDir Path dir) {
		// Given
		try (var dbOpts = new Options().setCreateIfMissing(true);
		     var db = RocksDB.open(dbOpts, dir);
		     var compact = new CompactOptions()
					 .setChangeLevel(true)
					 .setTargetLevel(-1)) {  // -1 = bottommost

			db.put("k".getBytes(), "v".getBytes());

			// When / Then — no throw
			db.compactRange(compact, null, null);

			assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// CompactOptions accessors
	// -----------------------------------------------------------------------

	@Test
	void compactOptions_defaults(@TempDir Path dir) {
		// Given / When
		try (var opts = new CompactOptions()) {
			// Then — verify getters don't throw and return consistent boolean values
			assertThat(opts.isExclusiveManualCompaction()).isIn(true, false);
			assertThat(opts.isBottommostLevelCompaction()).isIn(true, false);
			assertThat(opts.isChangeLevel()).isFalse();
		}
	}

	@Test
	void compactOptions_roundtrip() {
		// Given / When
		try (var opts = new CompactOptions()
				.setExclusiveManualCompaction(false)
				.setBottommostLevelCompaction(true)
				.setChangeLevel(true)
				.setTargetLevel(2)) {

			// Then
			assertThat(opts.isExclusiveManualCompaction()).isFalse();
			assertThat(opts.isBottommostLevelCompaction()).isTrue();
			assertThat(opts.isChangeLevel()).isTrue();
			assertThat(opts.getTargetLevel()).isEqualTo(2);
		}
	}

	// -----------------------------------------------------------------------
	// suggestCompactRange
	// -----------------------------------------------------------------------

	@Test
	void suggestCompactRange_doesNotThrow(@TempDir Path dir) {
		// Given
		try (var opts = new Options().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {
			db.put("a".getBytes(), "1".getBytes());
			db.put("z".getBytes(), "2".getBytes());

			// When / Then — hint only; no guarantee of compaction, no exception
			db.suggestCompactRange("a".getBytes(), "z".getBytes());
		}
	}

	@Test
	void suggestCompactRange_nullBounds(@TempDir Path dir) {
		// Given
		try (var opts = new Options().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {
			db.put("k".getBytes(), "v".getBytes());

			// When / Then
			db.suggestCompactRange(null, null);
		}
	}

	// -----------------------------------------------------------------------
	// disableFileDeletions / enableFileDeletions
	// -----------------------------------------------------------------------

	@Test
	void disableAndEnableFileDeletions_doesNotThrow(@TempDir Path dir) {
		// Given
		try (var opts = new Options().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {
			db.put("k".getBytes(), "v".getBytes());

			// When — disable, do some work, re-enable
			db.disableFileDeletions();
			db.put("k2".getBytes(), "v2".getBytes());
			db.enableFileDeletions();

			// Then
			assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
			assertThat(db.get("k2".getBytes())).isEqualTo("v2".getBytes());
		}
	}
}
