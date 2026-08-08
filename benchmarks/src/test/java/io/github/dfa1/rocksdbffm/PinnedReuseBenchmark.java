package io.github.dfa1.rocksdbffm;

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
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/// Does reusing one caller-owned slice across several gets beat allocating per get?
///
/// **The native allocation cannot be avoided either way.** `rocksdb_get_pinned` does
/// `new (rocksdb_pinnableslice_t)` on every call and `c.h` exposes no
/// `rocksdb_pinnableslice_create`, so the C++ pattern of one reused `PinnableSlice`
/// with `Reset()` between gets is not reachable through the C API. What reuse can
/// save is strictly the Java side: the wrapper, the [PinnedResult.Found] record, the
/// confined [Arena] that scopes the borrowed segment, and the length holder.
///
/// So this measures the **ceiling** on any reuse API:
///
///   - `perGet` — today's public API. One arena, one wrapper and one record per get.
///   - `reused` — everything hoisted out of the loop: one arena, one error holder and
///     one length holder for the whole batch, zero Java allocation per get. This is
///     what a caller-owned reused slice would compile down to at best.
///
/// `gets` is the number of reads per benchmark invocation, so the 1-vs-2-vs-many
/// question is answered directly. If the two converge, a reuse API buys nothing and
/// the per-get shape stays; if `reused` pulls ahead as `gets` grows, the gap is what
/// such an API would be worth — and the price is the borrowed segment losing its
/// scope, which is the use-after-free protection issue #45 was filed to add.
///
/// Lives in the library's own package to reach the package-private experiment hooks.
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 3, jvmArgsPrepend = {"--enable-native-access=ALL-UNNAMED", "--sun-misc-unsafe-memory-access=allow"})
public class PinnedReuseBenchmark {

	/// Reads performed per invocation, doubling, so any amortization shows up as a
	/// trend rather than two points. 1 is the control: if reuse only pays off across a
	/// batch, the gain at 1 should be nil.
	@Param({"1", "2", "4", "8", "16", "32"})
	public int gets;

	/// Value size in bytes, spanning the realistic range: 8 is a timestamp or counter,
	/// 64 a small record, 4096 a page, 65536 a blob. The pinned path is dominated by
	/// fixed overhead at the small end — which is exactly where per-get allocation
	/// shows up — and by data movement at the large end.
	@Param({"8", "64", "4096", "65536"})
	public int valueSize;

	private static final int KEY_COUNT = 32;

	private ReadWriteDB db;
	private Path dbPath;

	private Arena arena;
	private MemorySegment[] keys;

	/// Hoisted scratch for the reused variant — allocated once for the whole trial.
	private MemorySegment err;
	private MemorySegment lenHolder;

	@Setup(Level.Trial)
	public void setup() throws Exception {
		dbPath = Files.createTempDirectory("bench-reuse-");
		db = RocksDB.open(dbPath);

		byte[] value = new byte[valueSize];
		for (int i = 0; i < valueSize; i++) {
			value[i] = (byte) i;
		}

		arena = Arena.ofConfined();
		keys = new MemorySegment[KEY_COUNT];
		for (int i = 0; i < KEY_COUNT; i++) {
			byte[] key = ("reuse-key-" + i).getBytes();
			db.put(key, value);
			keys[i] = arena.allocateFrom(ValueLayout.JAVA_BYTE, key);
		}

		err = RocksDB.errHolder(arena);
		lenHolder = arena.allocate(ValueLayout.JAVA_LONG);
	}

	@TearDown(Level.Trial)
	public void teardown() throws IOException {
		db.close();
		arena.close();
		try (Stream<Path> paths = Files.walk(dbPath)) {
			paths.sorted(Comparator.reverseOrder()).forEach(p -> {
				try {
					Files.delete(p);
				} catch (IOException e) {
					throw new java.io.UncheckedIOException(e);
				}
			});
		}
	}

	/// Today's API: a fresh arena, wrapper and record for every get.
	@Benchmark
	public long perGet() {
		long sum = 0;
		for (int i = 0; i < gets; i++) {
			try (PinnedResult result = db.getPinned(keys[i & (KEY_COUNT - 1)])) {
				sum += switch (result) {
					case PinnedResult.Found found -> found.value().get(ValueLayout.JAVA_BYTE, 0);
					case PinnedResult.NotFound ignored -> 0;
				};
			}
		}
		return sum;
	}

	/// The ceiling: nothing allocated per get, and the value segment carries no scope.
	@Benchmark
	public long reused() {
		long sum = 0;
		for (int i = 0; i < gets; i++) {
			MemorySegment pin = db.getPinnedRaw(keys[i & (KEY_COUNT - 1)], err);
			if (!MemorySegment.NULL.equals(pin)) {
				try {
					sum += PinnableSlice.valueInto(pin, lenHolder).get(ValueLayout.JAVA_BYTE, 0);
				} finally {
					PinnableSlice.destroy(pin);
				}
			}
		}
		return sum;
	}

	static void main() throws Exception {
		org.openjdk.jmh.runner.options.Options opt = new OptionsBuilder()
				.include(PinnedReuseBenchmark.class.getSimpleName())
				.build();

		new org.openjdk.jmh.runner.Runner(opt).run();
	}
}
