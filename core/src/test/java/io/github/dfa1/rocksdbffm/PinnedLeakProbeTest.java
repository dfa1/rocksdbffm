package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class PinnedLeakProbeTest {

	@Test
	void closingFromAnotherThreadStillReleasesThePin(@TempDir Path dir) throws Exception {
		// Given a pinned result whose scoping arena is confined to this thread
		try (var db = RocksDB.open(dir)) {
			db.put("key".getBytes(), "value".getBytes());
			PinnedResult result = db.getPinned("key".getBytes());
			MemorySegment borrowed = ((PinnedResult.Found) result).value();

			// When it is closed from a different thread
			Thread other = new Thread(result::close);
			other.start();
			other.join();

			// Then the arena really did close, so the borrowed segment is dead. If it is
			// still readable, Arena.close() threw WrongThreadException, NativeObject.close()
			// swallowed it, and rocksdb_pinnableslice_destroy was skipped — leaking the
			// block-cache pin with the pointer already nulled, so nothing can retry it.
			boolean stillReadable;
			try {
				borrowed.get(ValueLayout.JAVA_BYTE, 0);
				stillReadable = true;
			} catch (RuntimeException expected) {
				stillReadable = false;
			}
			assertThat(stillReadable)
					.as("borrowed segment must be invalidated, proving the native slice was destroyed")
					.isFalse();
		}
	}
}
