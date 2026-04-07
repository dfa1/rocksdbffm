package io.github.dfa1.rocksdbffm.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Shared constants and helpers used by both {@link FfmBenchmark} and {@link JniBenchmark}.
 */
final class TestData {

	static final int BATCH_SIZE = 100;

	static final byte[] READ_KEY_BYTES = "read-key".getBytes();
	static final byte[] READ_VALUE_BYTES = "read-value-data-0123456789".getBytes();
	static final byte[] WRITE_KEY_BYTES = "bench-key".getBytes();
	static final byte[] WRITE_VALUE_BYTES = "bench-value-data-0123456789".getBytes();
	static final byte[] BATCH_VALUE = "batch-value-data-0123456789".getBytes();

	static byte[][] batchKeys() {
		byte[][] keys = new byte[BATCH_SIZE][];
		for (int i = 0; i < BATCH_SIZE; i++) {
			keys[i] = ("batch-key-" + i).getBytes();
		}
		return keys;
	}

	static void deleteDir(Path dir) throws IOException {
		Files.walk(dir)
				.sorted(Comparator.reverseOrder())
				.forEach(p -> p.toFile().delete());
	}

	private TestData() {
	}
}
