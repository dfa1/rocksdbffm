package io.github.dfa1.rocksdbffm.benchmark;

import io.github.dfa1.rocksdbffm.PinnedResult;
import io.github.dfa1.rocksdbffm.ReadWriteDB;
import io.github.dfa1.rocksdbffm.RocksDB;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/// Compares the pinned read path against the copying tiers across value sizes.
///
/// The interesting variable is value size: `getPinned` avoids a `memcpy` of the
/// whole value, so any advantage should grow with it and be invisible at the
/// 26-byte values [FfmBenchmark] uses.
///
/// Two consumption patterns are measured, because they answer different questions:
///
///   - **peek** — read a few bytes out of the value (a header, a tag, a length
///     prefix). The copying tiers must still materialize the entire value to hand
///     you any of it; the pinned path does not.
///   - **full** — get every byte onto the Java heap. Here the copy is unavoidable
///     and the pinned path only moves who performs it, so the tiers should converge.
///
/// There is no JNI column: rocksdbjni exposes no `PinnableSlice` to Java, so the
/// borrowed-value shape has no counterpart to compare against.
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 1, jvmArgsPrepend = {"--enable-native-access=ALL-UNNAMED", "--sun-misc-unsafe-memory-access=allow"})
public class PinnedReadBenchmark {

	/// Value sizes in bytes: small enough to be dominated by call overhead, through
	/// to large enough that the copy dominates.
	@Param({"32", "256", "4096", "65536"})
	public int valueSize;

	private static final byte[] KEY = "pinned-read-key".getBytes();

	private ReadWriteDB db;
	private Path dbPath;

	private Arena arena;
	private MemorySegment keySegment;
	/// Destination for the copy-into tier, sized to the current parameter.
	private MemorySegment destSegment;

	@Setup(Level.Trial)
	public void setup() throws Exception {
		dbPath = Files.createTempDirectory("bench-pinned-");
		db = RocksDB.open(dbPath);

		byte[] value = new byte[valueSize];
		for (int i = 0; i < valueSize; i++) {
			value[i] = (byte) i;
		}
		db.put(KEY, value);

		arena = Arena.ofConfined();
		keySegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, KEY);
		destSegment = arena.allocate(valueSize);
	}

	@TearDown(Level.Trial)
	public void teardown() throws IOException {
		db.close();
		arena.close();
		TestData.deleteDir(dbPath);
	}

	// ---- peek: only a few bytes of the value are actually wanted --------------

	/// Copying tier: materializes the whole value on the heap to read two bytes of it.
	@Benchmark
	public int peekViaBytes() {
		byte[] value = db.get(KEY);
		return value[0] + value[value.length - 1];
	}

	/// Copying tier: `memcpy`s the whole value into a native destination to read two bytes.
	@Benchmark
	public int peekViaSegmentCopy() {
		long len = db.get(keySegment, destSegment);
		return destSegment.get(ValueLayout.JAVA_BYTE, 0)
				+ destSegment.get(ValueLayout.JAVA_BYTE, len - 1);
	}

	/// Pinned tier: reads the two bytes straight out of the block cache.
	@Benchmark
	public int peekViaPinned() {
		try (PinnedResult result = db.getPinned(keySegment)) {
			return switch (result) {
				case PinnedResult.Found found -> {
					MemorySegment value = found.value();
					yield value.get(ValueLayout.JAVA_BYTE, 0)
							+ value.get(ValueLayout.JAVA_BYTE, value.byteSize() - 1);
				}
				case PinnedResult.NotFound ignored -> 0;
			};
		}
	}

	// ---- full: every byte is wanted on the Java heap --------------------------

	/// Copying tier: the existing `byte[]` read.
	@Benchmark
	public byte[] fullViaBytes() {
		return db.get(KEY);
	}

	/// Pinned tier, then copied out — the copy still happens, just later.
	@Benchmark
	public byte[] fullViaPinnedToArray() {
		try (PinnedResult result = db.getPinned(keySegment)) {
			return switch (result) {
				case PinnedResult.Found found -> found.toArray();
				case PinnedResult.NotFound ignored -> null;
			};
		}
	}

	// ---- lookup only: no value bytes are read ---------------------------------

	/// Pinned tier reading only the length. No value bytes are touched, but this is
	/// **not** a measure of bare call overhead: `value()` allocates the confined arena
	/// that scopes the borrowed segment, so the arena cost is included here — and in
	/// every other pinned benchmark above. That allocation is the most likely reason
	/// the pinned path loses to `peekViaSegmentCopy` at small value sizes, where there
	/// is almost nothing to copy.
	@Benchmark
	public long lookupViaPinned() {
		try (PinnedResult result = db.getPinned(keySegment)) {
			return switch (result) {
				case PinnedResult.Found found -> found.value().byteSize();
				case PinnedResult.NotFound ignored -> -1L;
			};
		}
	}

	static void main() throws Exception {
		org.openjdk.jmh.runner.options.Options opt = new OptionsBuilder()
				.include(PinnedReadBenchmark.class.getSimpleName())
				.build();

		new org.openjdk.jmh.runner.Runner(opt).run();
	}
}
