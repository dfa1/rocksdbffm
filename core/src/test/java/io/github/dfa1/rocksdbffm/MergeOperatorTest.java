package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class MergeOperatorTest {

	// -----------------------------------------------------------------------
	// Built-in: uint64 add
	// -----------------------------------------------------------------------

	@Test
	void uint64Add_accumulatesOperands(@TempDir Path dir) {
		// Given
		try (var opts = new Options().setCreateIfMissing(true).setUint64AddMergeOperator();
		     var db = RocksDB.open(opts, dir)) {

			db.put("counter".getBytes(), uint64(10));

			// When — two merge operands
			db.merge("counter".getBytes(), uint64(5));
			db.merge("counter".getBytes(), uint64(3));

			// Then — 10 + 5 + 3 = 18
			assertThat(fromUint64(db.get("counter".getBytes()))).isEqualTo(18L);
		}
	}

	@Test
	void uint64Add_noExistingValue_startsFromFirstOperand(@TempDir Path dir) {
		// Given
		try (var opts = new Options().setCreateIfMissing(true).setUint64AddMergeOperator();
		     var db = RocksDB.open(opts, dir)) {

			// When — merge with no prior put
			db.merge("k".getBytes(), uint64(7));
			db.merge("k".getBytes(), uint64(3));

			// Then
			assertThat(fromUint64(db.get("k".getBytes()))).isEqualTo(10L);
		}
	}

	// -----------------------------------------------------------------------
	// Custom MergeOperator — string append
	// -----------------------------------------------------------------------

	@Test
	void customOperator_appendsOperands(@TempDir Path dir) {
		// Given — operator that concatenates strings with ","
		try (var op = appendOperator(",");
		     var opts = new Options().setCreateIfMissing(true).setMergeOperator(op);
		     var db = RocksDB.open(opts, dir)) {

			db.put("k".getBytes(), "base".getBytes());

			// When
			db.merge("k".getBytes(), "a".getBytes());
			db.merge("k".getBytes(), "b".getBytes());

			// Then
			assertThat(new String(db.get("k".getBytes()))).isEqualTo("base,a,b");
		}
	}

	@Test
	void customOperator_noExistingValue(@TempDir Path dir) {
		// Given
		try (var op = appendOperator(",");
		     var opts = new Options().setCreateIfMissing(true).setMergeOperator(op);
		     var db = RocksDB.open(opts, dir)) {

			// When — merge with no prior put (existingValue will be null)
			db.merge("k".getBytes(), "first".getBytes());
			db.merge("k".getBytes(), "second".getBytes());

			// Then
			assertThat(new String(db.get("k".getBytes()))).isEqualTo("first,second");
		}
	}

	// -----------------------------------------------------------------------
	// RocksDB.merge — ByteBuffer and MemorySegment tiers
	// -----------------------------------------------------------------------

	@Test
	void merge_byteBuffer(@TempDir Path dir) {
		// Given
		try (var op = appendOperator("+");
		     var opts = new Options().setCreateIfMissing(true).setMergeOperator(op);
		     var db = RocksDB.open(opts, dir)) {

			db.put("k".getBytes(), "x".getBytes());

			var key = ByteBuffer.allocateDirect(1).put((byte) 'k').flip();
			var value = ByteBuffer.allocateDirect(1).put((byte) 'y').flip();

			// When
			db.merge(key, value);

			// Then
			assertThat(new String(db.get("k".getBytes()))).isEqualTo("x+y");
		}
	}

	@Test
	void merge_memorySegment(@TempDir Path dir) {
		// Given
		try (var op = appendOperator("+");
		     var opts = new Options().setCreateIfMissing(true).setMergeOperator(op);
		     var db = RocksDB.open(opts, dir);
		     var arena = Arena.ofConfined()) {

			db.put("k".getBytes(), "x".getBytes());

			var key = arena.allocateFrom(ValueLayout.JAVA_BYTE, (byte) 'k');
			var value = arena.allocateFrom(ValueLayout.JAVA_BYTE, (byte) 'z');

			// When
			db.merge(key, value);

			// Then
			assertThat(new String(db.get("k".getBytes()))).isEqualTo("x+z");
		}
	}

	// -----------------------------------------------------------------------
	// WriteBatch.merge
	// -----------------------------------------------------------------------

	@Test
	void writeBatch_merge_byteArray(@TempDir Path dir) {
		// Given
		try (var op = appendOperator(",");
		     var opts = new Options().setCreateIfMissing(true).setMergeOperator(op);
		     var db = RocksDB.open(opts, dir);
		     var batch = WriteBatch.create()) {

			db.put("k".getBytes(), "base".getBytes());

			batch.merge("k".getBytes(), "x".getBytes());
			batch.merge("k".getBytes(), "y".getBytes());

			// When
			db.write(batch);

			// Then
			assertThat(new String(db.get("k".getBytes()))).isEqualTo("base,x,y");
		}
	}

	@Test
	void writeBatch_merge_byteBuffer(@TempDir Path dir) {
		// Given
		try (var op = appendOperator(",");
		     var opts = new Options().setCreateIfMissing(true).setMergeOperator(op);
		     var db = RocksDB.open(opts, dir);
		     var batch = WriteBatch.create()) {

			db.put("k".getBytes(), "v".getBytes());

			var key = ByteBuffer.allocateDirect(1).put((byte) 'k').flip();
			var val = ByteBuffer.allocateDirect(1).put((byte) 'w').flip();
			batch.merge(key, val);

			// When
			db.write(batch);

			// Then
			assertThat(new String(db.get("k".getBytes()))).isEqualTo("v,w");
		}
	}

	@Test
	void writeBatch_merge_memorySegment(@TempDir Path dir) {
		// Given
		try (var op = appendOperator(",");
		     var opts = new Options().setCreateIfMissing(true).setMergeOperator(op);
		     var db = RocksDB.open(opts, dir);
		     var batch = WriteBatch.create();
		     var arena = Arena.ofConfined()) {

			db.put("k".getBytes(), "v".getBytes());

			var key = arena.allocateFrom(ValueLayout.JAVA_BYTE, (byte) 'k');
			var val = arena.allocateFrom(ValueLayout.JAVA_BYTE, (byte) 'w');
			batch.merge(key, val);

			// When
			db.write(batch);

			// Then
			assertThat(new String(db.get("k".getBytes()))).isEqualTo("v,w");
		}
	}

	// -----------------------------------------------------------------------
	// Helpers
	// -----------------------------------------------------------------------

	/**
	 * Append merge operator: joins existing value and operands with {@code sep}.
	 */
	private static MergeOperator appendOperator(String sep) {
		return MergeOperator.create(
				"append-" + sep,
				(key, existing, operands) -> {
					var parts = new java.util.ArrayList<String>();
					if (existing != null) parts.add(new String(existing));
					operands.stream().map(String::new).forEach(parts::add);
					return String.join(sep, parts).getBytes();
				},
				(key, operands) -> null  // defer partial merge
		);
	}

	private static byte[] uint64(long value) {
		return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
	}

	private static long fromUint64(byte[] bytes) {
		return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
	}
}
