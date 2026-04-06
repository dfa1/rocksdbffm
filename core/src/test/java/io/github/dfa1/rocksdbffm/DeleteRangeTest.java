package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class DeleteRangeTest {

    // -----------------------------------------------------------------------
    // RocksDB.deleteRange — byte[]
    // -----------------------------------------------------------------------

    @Test
    void deleteRange_removesKeysInRange(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir)) {
            db.put("a".getBytes(), "1".getBytes());
            db.put("b".getBytes(), "2".getBytes());
            db.put("c".getBytes(), "3".getBytes());
            db.put("d".getBytes(), "4".getBytes());

            // When — delete [b, d) i.e. b and c but not d
            db.deleteRange("b".getBytes(), "d".getBytes());

            // Then
            assertThat(db.get("a".getBytes())).isEqualTo("1".getBytes());
            assertThat(db.get("b".getBytes())).isNull();
            assertThat(db.get("c".getBytes())).isNull();
            assertThat(db.get("d".getBytes())).isEqualTo("4".getBytes());
        }
    }

    @Test
    void deleteRange_emptyRange_deletesNothing(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir)) {
            db.put("a".getBytes(), "1".getBytes());

            // When — start == end is an empty range
            db.deleteRange("a".getBytes(), "a".getBytes());

            // Then
            assertThat(db.get("a".getBytes())).isEqualTo("1".getBytes());
        }
    }

    @Test
    void deleteRange_rangeExceedsExistingKeys_deletesOnlyPresent(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir)) {
            db.put("b".getBytes(), "2".getBytes());

            // When — range extends beyond existing keys
            db.deleteRange("a".getBytes(), "z".getBytes());

            // Then
            assertThat(db.get("b".getBytes())).isNull();
        }
    }

    // -----------------------------------------------------------------------
    // RocksDB.deleteRange — ByteBuffer
    // -----------------------------------------------------------------------

    @Test
    void deleteRange_byteBuffer_removesKeysInRange(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir)) {
            db.put("a".getBytes(), "1".getBytes());
            db.put("b".getBytes(), "2".getBytes());
            db.put("c".getBytes(), "3".getBytes());

            var start = ByteBuffer.allocateDirect(1).put((byte) 'b').flip();
            var end   = ByteBuffer.allocateDirect(1).put((byte) 'c').flip();

            // When
            db.deleteRange(start, end);

            // Then
            assertThat(db.get("a".getBytes())).isEqualTo("1".getBytes());
            assertThat(db.get("b".getBytes())).isNull();
            assertThat(db.get("c".getBytes())).isEqualTo("3".getBytes());
        }
    }

    // -----------------------------------------------------------------------
    // RocksDB.deleteRange — MemorySegment
    // -----------------------------------------------------------------------

    @Test
    void deleteRange_memorySegment_removesKeysInRange(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir);
             var arena = Arena.ofConfined()) {

            db.put("a".getBytes(), "1".getBytes());
            db.put("b".getBytes(), "2".getBytes());
            db.put("c".getBytes(), "3".getBytes());

            var start = arena.allocateFrom(ValueLayout.JAVA_BYTE, (byte) 'b');
            var end   = arena.allocateFrom(ValueLayout.JAVA_BYTE, (byte) 'c');

            // When
            db.deleteRange(start, end);

            // Then
            assertThat(db.get("a".getBytes())).isEqualTo("1".getBytes());
            assertThat(db.get("b".getBytes())).isNull();
            assertThat(db.get("c".getBytes())).isEqualTo("3".getBytes());
        }
    }

    // -----------------------------------------------------------------------
    // WriteBatch.deleteRange — byte[]
    // -----------------------------------------------------------------------

    @Test
    void writeBatch_deleteRange_removesKeysInRange(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir);
             var batch = WriteBatch.create()) {

            db.put("a".getBytes(), "1".getBytes());
            db.put("b".getBytes(), "2".getBytes());
            db.put("c".getBytes(), "3".getBytes());
            db.put("d".getBytes(), "4".getBytes());

            batch.deleteRange("b".getBytes(), "d".getBytes());

            // When
            db.write(batch);

            // Then
            assertThat(db.get("a".getBytes())).isEqualTo("1".getBytes());
            assertThat(db.get("b".getBytes())).isNull();
            assertThat(db.get("c".getBytes())).isNull();
            assertThat(db.get("d".getBytes())).isEqualTo("4".getBytes());
        }
    }

    @Test
    void writeBatch_deleteRange_combinedWithPuts(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir);
             var batch = WriteBatch.create()) {

            db.put("b".getBytes(), "old".getBytes());
            db.put("c".getBytes(), "old".getBytes());

            // When — range tombstone and a new put in the same batch
            batch.deleteRange("b".getBytes(), "d".getBytes());
            batch.put("e".getBytes(), "new".getBytes());
            db.write(batch);

            // Then
            assertThat(db.get("b".getBytes())).isNull();
            assertThat(db.get("c".getBytes())).isNull();
            assertThat(db.get("e".getBytes())).isEqualTo("new".getBytes());
        }
    }

    // -----------------------------------------------------------------------
    // WriteBatch.deleteRange — ByteBuffer
    // -----------------------------------------------------------------------

    @Test
    void writeBatch_deleteRange_byteBuffer(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir);
             var batch = WriteBatch.create()) {

            db.put("b".getBytes(), "2".getBytes());
            db.put("c".getBytes(), "3".getBytes());

            var start = ByteBuffer.allocateDirect(1).put((byte) 'b').flip();
            var end   = ByteBuffer.allocateDirect(1).put((byte) 'd').flip();

            batch.deleteRange(start, end);

            // When
            db.write(batch);

            // Then
            assertThat(db.get("b".getBytes())).isNull();
            assertThat(db.get("c".getBytes())).isNull();
        }
    }

    // -----------------------------------------------------------------------
    // WriteBatch.deleteRange — MemorySegment
    // -----------------------------------------------------------------------

    @Test
    void writeBatch_deleteRange_memorySegment(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir);
             var batch = WriteBatch.create();
             var arena = Arena.ofConfined()) {

            db.put("b".getBytes(), "2".getBytes());
            db.put("c".getBytes(), "3".getBytes());

            var start = arena.allocateFrom(ValueLayout.JAVA_BYTE, (byte) 'b');
            var end   = arena.allocateFrom(ValueLayout.JAVA_BYTE, (byte) 'd');

            batch.deleteRange(start, end);

            // When
            db.write(batch);

            // Then
            assertThat(db.get("b".getBytes())).isNull();
            assertThat(db.get("c".getBytes())).isNull();
        }
    }
}
