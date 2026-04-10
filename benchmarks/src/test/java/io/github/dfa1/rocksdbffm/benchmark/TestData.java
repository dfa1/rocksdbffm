package io.github.dfa1.rocksdbffm.benchmark;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/// Shared constants and helpers used by both [FfmBenchmark] and [JniBenchmark].
final class TestData {

	static final int WRITE_BATCH_SIZE = 100;

	static final byte[] READ_KEY_BYTES = "read-key".getBytes();
	static final byte[] READ_VALUE_BYTES = "read-value-data-0123456789".getBytes();
	static final byte[] WRITE_KEY_BYTES = "bench-key".getBytes();
	static final byte[] WRITE_VALUE_BYTES = "bench-value-data-0123456789".getBytes();
	static final byte[] BATCH_VALUE = "batch-value-data-0123456789".getBytes();

	static byte[][] batchKeys() {
		byte[][] keys = new byte[WRITE_BATCH_SIZE][];
		for (int i = 0; i < WRITE_BATCH_SIZE; i++) {
			keys[i] = ("batch-key-" + i).getBytes();
		}
		return keys;
	}

	static void deleteDir(Path root) throws IOException {
		try (Stream<Path> paths = Files.walk(root)) {
			paths.sorted(Comparator.reverseOrder())
					.forEach(path -> {
						try {
							Files.delete(path);
						} catch (IOException e) {
							throw new UncheckedIOException(e);
						}
					});
		}
	}

	private TestData() {
		// no instances
	}
}
