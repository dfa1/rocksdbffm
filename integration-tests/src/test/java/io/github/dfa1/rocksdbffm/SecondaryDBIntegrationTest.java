package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class SecondaryDBIntegrationTest {

	@Test
	void catchUp_seesInitialPrimaryWrites(
			@TempDir Path primaryDir, @TempDir Path secondaryDir) {
		// Given — write and flush so the secondary can read from SSTs
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, primaryDir);
		     var fo = FlushOptions.newFlushOptions()) {
			db.put("k1".getBytes(), "v1".getBytes());
			db.flush(fo);
		}

		// When
		try (var opts = Options.newOptions();
		     var secondary = RocksDB.openSecondary(opts, primaryDir, secondaryDir)) {
			secondary.tryCatchUpWithPrimary();

			// Then
			assertThat(secondary.get("k1".getBytes())).isEqualTo("v1".getBytes());
		}
	}

	@Test
	void catchUp_picksUpSubsequentPrimaryWrites(
			@TempDir Path primaryDir, @TempDir Path secondaryDir) {
		// Given — seed the primary so the secondary can open
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, primaryDir);
		     var fo = FlushOptions.newFlushOptions()) {
			db.put("k1".getBytes(), "v1".getBytes());
			db.flush(fo);
		}

		try (var opts = Options.newOptions();
		     var secondary = RocksDB.openSecondary(opts, primaryDir, secondaryDir)) {
			secondary.tryCatchUpWithPrimary();

			// When — primary adds more data and flushes
			try (var popts = Options.newOptions();
			     var primary = RocksDB.open(popts, primaryDir);
			     var fo = FlushOptions.newFlushOptions()) {
				primary.put("k2".getBytes(), "v2".getBytes());
				primary.flush(fo);
			}
			secondary.tryCatchUpWithPrimary();

			// Then — secondary sees both writes
			assertThat(secondary.get("k1".getBytes())).isEqualTo("v1".getBytes());
			assertThat(secondary.get("k2".getBytes())).isEqualTo("v2".getBytes());
		}
	}
}
