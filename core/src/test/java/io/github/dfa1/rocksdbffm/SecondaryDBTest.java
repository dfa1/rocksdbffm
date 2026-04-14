package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class SecondaryDBTest {

	// -----------------------------------------------------------------------
	// Open / close
	// -----------------------------------------------------------------------

	@Test
	void open_closesCleanly(@TempDir Path primaryDir, @TempDir Path secondaryDir) {
		// Given — primary must exist before opening a secondary
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var primary = RocksDB.open(opts, primaryDir)) {
			primary.put("seed".getBytes(), "value".getBytes());
		}

		// When — secondary opens against an existing primary
		try (var opts = Options.newOptions();
		     var secondary = RocksDB.openSecondary(opts, primaryDir, secondaryDir)) {

			// Then
			assertThat(secondary).isNotNull();
		}
	}

	// -----------------------------------------------------------------------
	// tryCatchUpWithPrimary
	// -----------------------------------------------------------------------

	@Test
	void tryCatchUpWithPrimary_doesNotThrow(@TempDir Path primaryDir, @TempDir Path secondaryDir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var primary = RocksDB.open(opts, primaryDir)) {
			primary.put("k".getBytes(), "v".getBytes());
		}

		try (var opts = Options.newOptions();
		     var secondary = RocksDB.openSecondary(opts, primaryDir, secondaryDir)) {

			// When
			secondary.tryCatchUpWithPrimary();

			// Then — no exception
		}
	}

	@Test
	void tryCatchUpWithPrimary_seesNewWrites(
			@TempDir Path primaryDir, @TempDir Path secondaryDir) throws Exception {
		// Given — write initial data to primary and flush so secondary can read it
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var primary = RocksDB.open(opts, primaryDir);
		     var fo = FlushOptions.newFlushOptions()) {
			primary.put("k1".getBytes(), "v1".getBytes());
			primary.flush(fo); // flush to SST so secondary can find it
		}

		// Open secondary, catch up, verify initial data
		try (var opts = Options.newOptions();
		     var secondary = RocksDB.openSecondary(opts, primaryDir, secondaryDir)) {

			secondary.tryCatchUpWithPrimary();
			assertThat(secondary.get("k1".getBytes())).isEqualTo("v1".getBytes());

			// When — primary writes more data and flushes
			try (var popts = Options.newOptions();
			     var primary = RocksDB.open(popts, primaryDir);
			     var fo = FlushOptions.newFlushOptions()) {
				primary.put("k2".getBytes(), "v2".getBytes());
				primary.flush(fo);
			}

			// Then — secondary picks it up after another catch-up
			secondary.tryCatchUpWithPrimary();
			assertThat(secondary.get("k2".getBytes())).isEqualTo("v2".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// Read operations
	// -----------------------------------------------------------------------

	@Test
	void get_returnsNull_whenKeyMissing(@TempDir Path primaryDir, @TempDir Path secondaryDir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var primary = RocksDB.open(opts, primaryDir)) {
			primary.put("seed".getBytes(), "x".getBytes());
		}

		try (var opts = Options.newOptions();
		     var secondary = RocksDB.openSecondary(opts, primaryDir, secondaryDir)) {
			secondary.tryCatchUpWithPrimary();

			// When
			var result = secondary.get("missing".getBytes());

			// Then
			assertThat(result).isNull();
		}
	}

	@Test
	void get_withReadOptions(@TempDir Path primaryDir, @TempDir Path secondaryDir) {
		// Given — write and flush
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var primary = RocksDB.open(opts, primaryDir);
		     var fo = FlushOptions.newFlushOptions()) {
			primary.put("k".getBytes(), "v".getBytes());
			primary.flush(fo);
		}

		try (var opts = Options.newOptions();
		     var secondary = RocksDB.openSecondary(opts, primaryDir, secondaryDir);
		     var ro = ReadOptions.newReadOptions()) {
			secondary.tryCatchUpWithPrimary();

			// When
			var result = secondary.get(ro, "k".getBytes());

			// Then
			assertThat(result).isEqualTo("v".getBytes());
		}
	}

	@Test
	void get_byteBuffer_returnsValue(@TempDir Path primaryDir, @TempDir Path secondaryDir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var primary = RocksDB.open(opts, primaryDir);
		     var fo = FlushOptions.newFlushOptions()) {
			primary.put("k".getBytes(), "v".getBytes());
			primary.flush(fo);
		}

		try (var opts = Options.newOptions();
		     var secondary = RocksDB.openSecondary(opts, primaryDir, secondaryDir)) {
			secondary.tryCatchUpWithPrimary();

			var key = ByteBuffer.allocateDirect(1);
			key.put("k".getBytes()).flip();
			var out = ByteBuffer.allocateDirect(32);

			// When
			int len = secondary.get(key, out);

			// Then
			assertThat(len).isEqualTo(1);
			out.flip();
			var bytes = new byte[out.remaining()];
			out.get(bytes);
			assertThat(bytes).isEqualTo("v".getBytes());
		}
	}

	@Test
	void get_memorySegment_returnsValue(@TempDir Path primaryDir, @TempDir Path secondaryDir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var primary = RocksDB.open(opts, primaryDir);
		     var fo = FlushOptions.newFlushOptions()) {
			primary.put("k".getBytes(), "v".getBytes());
			primary.flush(fo);
		}

		try (var opts = Options.newOptions();
		     var secondary = RocksDB.openSecondary(opts, primaryDir, secondaryDir);
		     Arena arena = Arena.ofConfined()) {
			secondary.tryCatchUpWithPrimary();

			var key = arena.allocateFrom("k");
			var out = arena.allocate(32);

			// When
			long len = secondary.get(key.asSlice(0, 1), out);

			// Then
			assertThat(len).isEqualTo(1);
			assertThat(out.asSlice(0, 1).toArray(ValueLayout.JAVA_BYTE)).isEqualTo("v".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// Iterator
	// -----------------------------------------------------------------------

	@Test
	void newIterator_iteratesData(@TempDir Path primaryDir, @TempDir Path secondaryDir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var primary = RocksDB.open(opts, primaryDir);
		     var fo = FlushOptions.newFlushOptions()) {
			primary.put("a".getBytes(), "1".getBytes());
			primary.put("b".getBytes(), "2".getBytes());
			primary.flush(fo);
		}

		try (var opts = Options.newOptions();
		     var secondary = RocksDB.openSecondary(opts, primaryDir, secondaryDir)) {
			secondary.tryCatchUpWithPrimary();

			// When
			try (var it = secondary.newIterator()) {
				it.seekToFirst();

				// Then
				assertThat(it.isValid()).isTrue();
				assertThat(it.key()).isEqualTo("a".getBytes());
				it.next();
				assertThat(it.isValid()).isTrue();
				assertThat(it.key()).isEqualTo("b".getBytes());
			}
		}
	}

	@Test
	void newIterator_withReadOptions(@TempDir Path primaryDir, @TempDir Path secondaryDir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var primary = RocksDB.open(opts, primaryDir);
		     var fo = FlushOptions.newFlushOptions()) {
			primary.put("x".getBytes(), "y".getBytes());
			primary.flush(fo);
		}

		try (var opts = Options.newOptions();
		     var secondary = RocksDB.openSecondary(opts, primaryDir, secondaryDir);
		     var ro = ReadOptions.newReadOptions()) {
			secondary.tryCatchUpWithPrimary();

			try (var it = secondary.newIterator(ro)) {
				it.seekToFirst();
				assertThat(it.isValid()).isTrue();
				assertThat(it.key()).isEqualTo("x".getBytes());
			}
		}
	}

	// -----------------------------------------------------------------------
	// Snapshot
	// -----------------------------------------------------------------------

	@Test
	void getSnapshot_allowsSnapshotRead(@TempDir Path primaryDir, @TempDir Path secondaryDir) {
		// Given — write and flush so secondary can catch up
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var primary = RocksDB.open(opts, primaryDir);
		     var fo = FlushOptions.newFlushOptions()) {
			primary.put("k".getBytes(), "v".getBytes());
			primary.flush(fo);
		}

		try (var opts = Options.newOptions();
		     var secondary = RocksDB.openSecondary(opts, primaryDir, secondaryDir)) {
			secondary.tryCatchUpWithPrimary();

			// When — take a snapshot and read through it
			try (var snap = secondary.getSnapshot();
			     var ro = ReadOptions.newReadOptions().setSnapshot(snap)) {

				// Then — snapshot read returns the value visible at snapshot time
				assertThat(secondary.get(ro, "k".getBytes())).isEqualTo("v".getBytes());
			}
		}
	}

	// -----------------------------------------------------------------------
	// DB Properties
	// -----------------------------------------------------------------------

	@Test
	void getLongProperty_returnsValue(@TempDir Path primaryDir, @TempDir Path secondaryDir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var primary = RocksDB.open(opts, primaryDir);
		     var fo = FlushOptions.newFlushOptions()) {
			primary.put("k".getBytes(), "v".getBytes());
			primary.flush(fo);
		}

		try (var opts = Options.newOptions();
		     var secondary = RocksDB.openSecondary(opts, primaryDir, secondaryDir)) {
			secondary.tryCatchUpWithPrimary();

			assertThat(secondary.getLongProperty(Property.ESTIMATE_NUM_KEYS)).isPresent();
		}
	}
}
