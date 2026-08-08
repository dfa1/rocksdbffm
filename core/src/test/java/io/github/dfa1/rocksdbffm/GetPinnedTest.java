package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GetPinnedTest {

	private static ByteBuffer direct(byte[] bytes) {
		return ByteBuffer.allocateDirect(bytes.length).put(bytes).flip();
	}

	private static byte[] bytesOf(MemorySegment segment) {
		return segment.toArray(ValueLayout.JAVA_BYTE);
	}

	// -----------------------------------------------------------------------
	// Found / NotFound
	// -----------------------------------------------------------------------

	@Test
	void getPinned_returnsFoundHoldingTheValue(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("key".getBytes(), "value".getBytes());

			// When
			try (PinnedResult result = db.getPinned("key".getBytes())) {

				// Then
				assertThat(result).isInstanceOf(PinnedResult.Found.class);
				assertThat(bytesOf(((PinnedResult.Found) result).value())).isEqualTo("value".getBytes());
			}
		}
	}

	@Test
	void getPinned_returnsNotFoundForAbsentKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("present".getBytes(), "value".getBytes());

			// When
			try (PinnedResult result = db.getPinned("absent".getBytes())) {

				// Then
				assertThat(result).isSameAs(PinnedResult.NotFound.INSTANCE);
			}
		}
	}

	@Test
	void getPinned_distinguishesEmptyValueFromAbsentKey(@TempDir Path dir) {
		// Given — the case the segment-tier get cannot express (#44): both report 0
		try (var db = RocksDB.open(dir)) {
			db.put("present-empty".getBytes(), new byte[0]);

			// When
			try (PinnedResult present = db.getPinned("present-empty".getBytes());
			     PinnedResult absent = db.getPinned("absent".getBytes())) {

				// Then
				assertThat(present).isInstanceOf(PinnedResult.Found.class);
				assertThat(((PinnedResult.Found) present).value().byteSize()).isZero();
				assertThat(absent).isSameAs(PinnedResult.NotFound.INSTANCE);
			}
		}
	}

	@Test
	void getPinned_switchOverResultIsExhaustive(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("key".getBytes(), "value".getBytes());

			// When — the idiomatic call site: no sentinel to compare, no branch to forget
			String outcome;
			try (PinnedResult result = db.getPinned("key".getBytes())) {
				outcome = switch (result) {
					case PinnedResult.Found found -> new String(bytesOf(found.value()));
					case PinnedResult.NotFound ignored -> "miss";
				};
			}

			// Then
			assertThat(outcome).isEqualTo("value");
		}
	}

	// -----------------------------------------------------------------------
	// Value length is intrinsic to the segment
	// -----------------------------------------------------------------------

	@Test
	void foundValue_carriesItsOwnLength(@TempDir Path dir) {
		// Given — a value long enough that a wrong length would be obvious
		byte[] value = "0123456789abcdefghijklmnopqrstuv".getBytes();
		try (var db = RocksDB.open(dir)) {
			db.put("key".getBytes(), value);

			// When
			try (PinnedResult result = db.getPinned("key".getBytes())) {
				MemorySegment segment = ((PinnedResult.Found) result).value();

				// Then — no caller-supplied capacity to compare against, so no truncation
				assertThat(segment.byteSize()).isEqualTo(value.length);
				assertThat(bytesOf(segment)).isEqualTo(value);
			}
		}
	}

	@Test
	void foundValue_isStableAcrossRepeatedCalls(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("key".getBytes(), "value".getBytes());

			// When
			try (PinnedResult result = db.getPinned("key".getBytes())) {
				PinnedResult.Found found = (PinnedResult.Found) result;
				MemorySegment first = found.value();
				MemorySegment second = found.value();

				// Then
				assertThat(second).isSameAs(first);
			}
		}
	}

	@Test
	void found_toArrayCopiesOntoTheHeap(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("key".getBytes(), "value".getBytes());

			// When
			byte[] copied;
			try (PinnedResult result = db.getPinned("key".getBytes())) {
				copied = ((PinnedResult.Found) result).toArray();
			}

			// Then — the copy outlives the pin
			assertThat(copied).isEqualTo("value".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// Lifetime
	// -----------------------------------------------------------------------

	@Test
	void pinnedValue_isUnusableAfterClose(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("key".getBytes(), "value".getBytes());
			MemorySegment escaped;
			try (PinnedResult result = db.getPinned("key".getBytes())) {
				escaped = ((PinnedResult.Found) result).value();
			}

			// When / Then — reading freed memory throws instead of returning garbage
			assertThatThrownBy(() -> bytesOf(escaped))
					.isInstanceOf(IllegalStateException.class);
		}
	}

	@Test
	void found_closeIsIdempotent(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("key".getBytes(), "value".getBytes());
			PinnedResult result = db.getPinned("key".getBytes());

			// When — a second close must not double-free the native slice
			result.close();
			result.close();

			// Then
			assertThatThrownBy(() -> ((PinnedResult.Found) result).value())
					.isInstanceOf(IllegalStateException.class);
		}
	}

	@Test
	void notFound_closeIsHarmless(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {

			// When
			PinnedResult result = db.getPinned("absent".getBytes());
			result.close();
			result.close();

			// Then
			assertThat(result).isSameAs(PinnedResult.NotFound.INSTANCE);
		}
	}

	// -----------------------------------------------------------------------
	// Key tiers
	// -----------------------------------------------------------------------

	@Test
	void getPinned_acceptsByteBufferKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("key".getBytes(), "value".getBytes());

			// When
			try (PinnedResult result = db.getPinned(direct("key".getBytes()))) {

				// Then
				assertThat(bytesOf(((PinnedResult.Found) result).value())).isEqualTo("value".getBytes());
			}
		}
	}

	@Test
	void getPinned_acceptsMemorySegmentKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     Arena arena = Arena.ofConfined()) {
			db.put("key".getBytes(), "value".getBytes());
			MemorySegment key = arena.allocateFrom(ValueLayout.JAVA_BYTE, "key".getBytes());

			// When
			try (PinnedResult result = db.getPinned(key)) {

				// Then
				assertThat(bytesOf(((PinnedResult.Found) result).value())).isEqualTo("value".getBytes());
			}
		}
	}

	@Test
	void getPinned_honorsReadOptionsSnapshot(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("key".getBytes(), "before".getBytes());

			// When — take a snapshot, overwrite, then read pinned through the snapshot
			try (Snapshot snap = db.getSnapshot();
			     ReadOptions ro = ReadOptions.newReadOptions().setSnapshot(snap)) {
				db.put("key".getBytes(), "after".getBytes());

				try (PinnedResult pinned = db.getPinned(ro, "key".getBytes());
				     PinnedResult live = db.getPinned("key".getBytes())) {

					// Then
					assertThat(bytesOf(((PinnedResult.Found) pinned).value())).isEqualTo("before".getBytes());
					assertThat(bytesOf(((PinnedResult.Found) live).value())).isEqualTo("after".getBytes());
				}
			}
		}
	}

	// -----------------------------------------------------------------------
	// Column families
	// -----------------------------------------------------------------------

	@Test
	void getPinned_readsFromColumnFamily(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("users"))) {
			db.put(cf, "alice".getBytes(), "data".getBytes());

			// When
			try (PinnedResult inCf = db.getPinned(cf, "alice".getBytes());
			     PinnedResult inDefault = db.getPinned("alice".getBytes())) {

				// Then — the CF read finds it, the default CF does not
				assertThat(bytesOf(((PinnedResult.Found) inCf).value())).isEqualTo("data".getBytes());
				assertThat(inDefault).isSameAs(PinnedResult.NotFound.INSTANCE);
			}
		}
	}

	@Test
	void getPinned_readsFromColumnFamilyWithSegmentKey(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("users"));
		     Arena arena = Arena.ofConfined()) {
			db.put(cf, "alice".getBytes(), "data".getBytes());
			MemorySegment key = arena.allocateFrom(ValueLayout.JAVA_BYTE, "alice".getBytes());

			// When
			try (PinnedResult result = db.getPinned(cf, key)) {

				// Then
				assertThat(bytesOf(((PinnedResult.Found) result).value())).isEqualTo("data".getBytes());
			}
		}
	}

	// -----------------------------------------------------------------------
	// Other database types
	// -----------------------------------------------------------------------

	@Test
	void getPinned_worksOnReadOnlyDB(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir)) {
			db.put("key".getBytes(), "value".getBytes());
		}

		// When
		try (var opts = Options.newOptions();
		     var db = RocksDB.openReadOnly(opts, dir);
		     PinnedResult result = db.getPinned("key".getBytes())) {

			// Then
			assertThat(bytesOf(((PinnedResult.Found) result).value())).isEqualTo("value".getBytes());
		}
	}

	@Test
	void getPinned_worksOnTransactionDB(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var txnOpts = TransactionDBOptions.newTransactionDBOptions();
		     var db = RocksDB.openTransaction(opts, txnOpts, dir)) {
			db.put("key".getBytes(), "value".getBytes());

			// When
			try (PinnedResult found = db.getPinned("key".getBytes());
			     PinnedResult missing = db.getPinned("absent".getBytes())) {

				// Then
				assertThat(bytesOf(((PinnedResult.Found) found).value())).isEqualTo("value".getBytes());
				assertThat(missing).isSameAs(PinnedResult.NotFound.INSTANCE);
			}
		}
	}

	@Test
	void getPinned_worksInsideTransaction(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var txnOpts = TransactionDBOptions.newTransactionDBOptions();
		     var db = RocksDB.openTransaction(opts, txnOpts, dir);
		     var wo = WriteOptions.newWriteOptions();
		     var ro = ReadOptions.newReadOptions()) {

			// When — an uncommitted write is visible to the transaction's own pinned read
			try (Transaction txn = db.beginTransaction(wo)) {
				txn.put("key".getBytes(), "staged".getBytes());

				try (PinnedResult result = txn.getPinned(ro, "key".getBytes())) {

					// Then
					assertThat(bytesOf(((PinnedResult.Found) result).value())).isEqualTo("staged".getBytes());
				}
				txn.rollback();
			}
		}
	}

	@Test
	void getPinned_worksOnTtlDB(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.openWithTtl(opts, dir, java.time.Duration.ofHours(1))) {
			db.put("key".getBytes(), "value".getBytes());

			// When
			try (PinnedResult result = db.getPinned("key".getBytes())) {

				// Then
				assertThat(bytesOf(((PinnedResult.Found) result).value())).isEqualTo("value".getBytes());
			}
		}
	}

	@Test
	void getPinned_worksOnOptimisticTransactionDB(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.openOptimistic(opts, dir)) {
			db.put("key".getBytes(), "value".getBytes());

			// When
			try (PinnedResult result = db.getPinned("key".getBytes())) {

				// Then
				assertThat(bytesOf(((PinnedResult.Found) result).value())).isEqualTo("value".getBytes());
			}
		}
	}

	@Test
	void getPinned_readsAcrossReopenedColumnFamilies(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dir);
		     var cf = db.createColumnFamily(ColumnFamilyDescriptor.of("users"))) {
			db.put(cf, "alice".getBytes(), "data".getBytes());
		}

		// When
		List<ColumnFamilyHandle> handles = new ArrayList<>();
		try (var opts = Options.newOptions().setCreateIfMissing(false);
		     var db = RocksDB.openWithColumnFamilies(opts, dir,
				     List.of(ColumnFamilyDescriptor.of("default"),
						     ColumnFamilyDescriptor.of("users")),
				     handles)) {

			try (PinnedResult result = db.getPinned(handles.get(1), "alice".getBytes())) {

				// Then
				assertThat(bytesOf(((PinnedResult.Found) result).value())).isEqualTo("data".getBytes());
			}
			handles.forEach(ColumnFamilyHandle::close);
		}
	}
}
