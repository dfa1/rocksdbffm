package io.github.dfa1.rocksdbffm.benchmark;

import io.github.dfa1.rocksdbffm.RocksDB;
import io.github.dfa1.rocksdbffm.WriteBatch;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsPrepend = {"--enable-native-access=ALL-UNNAMED", "--sun-misc-unsafe-memory-access=allow"})
public class FfmBenchmark {

	private static final int BATCH_SIZE = 10000;
	private static final byte[] READ_KEY_BYTES = "read-key".getBytes();
	private static final byte[] READ_VALUE_BYTES = "read-value-data-0123456789".getBytes();
	private static final byte[] WRITE_KEY_BYTES = "bench-key".getBytes();
	private static final byte[] WRITE_VALUE_BYTES = "bench-value-data-0123456789".getBytes();
	private static final byte[] BATCH_VALUE = "batch-value-data-0123456789".getBytes();

	private RocksDB db;
	private Path dbPath;

	// byte[] tier
	private byte[] writeKeyBytes;
	private byte[] writeValueBytes;
	private byte[] readKeyBytes;

	// ByteBuffer tier
	private ByteBuffer writeKeyByteBuffer;
	private ByteBuffer writeValByteBuffer;
	private ByteBuffer readKeyByteBuffer;
	private ByteBuffer readValByteBuffer;

	// MemorySegment tier — confined arena held open for the full trial lifetime
	private Arena arenaMemorySegment;
	private MemorySegment writeKeyMemorySegment;
	private MemorySegment writeValueMemorySegment;
	private MemorySegment readKeyMemorySegment;
	private MemorySegment readValMemorySegment;

	private WriteBatch batch;
	private byte[][] batchKeys;

	@Setup(Level.Trial)
	public void setup() throws Exception {
		dbPath = Files.createTempDirectory("bench-ffm-");
		db = RocksDB.open(dbPath);

		// --- byte[] tier ---
		writeKeyBytes = WRITE_KEY_BYTES.clone();
		writeValueBytes = WRITE_VALUE_BYTES.clone();
		readKeyBytes = READ_KEY_BYTES.clone();

		// --- ByteBuffer tier ---
		writeKeyByteBuffer = ByteBuffer.allocateDirect(WRITE_KEY_BYTES.length);
		writeKeyByteBuffer.put(WRITE_KEY_BYTES).flip();
		writeValByteBuffer = ByteBuffer.allocateDirect(WRITE_VALUE_BYTES.length);
		writeValByteBuffer.put(WRITE_VALUE_BYTES).flip();
		readKeyByteBuffer = ByteBuffer.allocateDirect(READ_KEY_BYTES.length);
		readKeyByteBuffer.put(READ_KEY_BYTES).flip();
		readValByteBuffer = ByteBuffer.allocateDirect(64);

		// --- MemorySegment tier ---
		arenaMemorySegment = Arena.ofConfined();
		writeKeyMemorySegment = arenaMemorySegment.allocateFrom(ValueLayout.JAVA_BYTE, WRITE_KEY_BYTES);
		writeValueMemorySegment = arenaMemorySegment.allocateFrom(ValueLayout.JAVA_BYTE, WRITE_VALUE_BYTES);
		readKeyMemorySegment = arenaMemorySegment.allocateFrom(ValueLayout.JAVA_BYTE, READ_KEY_BYTES);
		readValMemorySegment = arenaMemorySegment.allocate(64);

		// Seed the read key
		db.put(READ_KEY_BYTES, READ_VALUE_BYTES);

		// --- batch ---
		batchKeys = new byte[BATCH_SIZE][];
		for (int i = 0; i < BATCH_SIZE; i++) {
			batchKeys[i] = ("batch-key-" + i).getBytes();
		}
		batch = WriteBatch.create();
	}

	@TearDown(Level.Trial)
	public void teardown() throws IOException {
		batch.close();
		db.close();
		arenaMemorySegment.close();
		deleteDir(dbPath);
	}

	// ---- byte[] tier -------------------------------------------------------

	private static final Arena ARENA = Arena.ofAuto();

	@Benchmark
	public void writesBytes() {
		db.put(writeKeyBytes, writeValueBytes);
	}

	@Benchmark
	public void writesBytesArena() {
		db.put(ARENA, writeKeyBytes, writeValueBytes);
	}

	@Benchmark
	public byte[] readsBytes() {
		return db.get(readKeyBytes);
	}

	// ---- ByteBuffer tier ---------------------------------------------------

	@Benchmark
	public void writesDirectByteBuffer() {
		writeKeyByteBuffer.rewind();
		writeValByteBuffer.rewind();
		db.put(writeKeyByteBuffer, writeValByteBuffer);
	}

	@Benchmark
	public int readsDirectByteBuffer() {
		readKeyByteBuffer.rewind();
		readValByteBuffer.clear();
		return db.get(readKeyByteBuffer, readValByteBuffer);
	}

	// ---- MemorySegment tier (FFM-only) ------------------------------------
	@Benchmark
	public void writesMemorySegment2() {
		db.put2(writeKeyMemorySegment, writeValueMemorySegment);
	}

	@Benchmark
	public void writesMemorySegmentArena() {
		db.put(ARENA, writeKeyMemorySegment, writeValueMemorySegment);
	}

	@Benchmark
	public void writesMemorySegment() {
		db.put(writeKeyMemorySegment, writeValueMemorySegment);
	}

	@Benchmark
	public long readsMemorySegment() {
		return db.get(readKeyMemorySegment, readValMemorySegment);
	}

	// ---- batch (byte[] keys, same as JNI) ---------------------------------

	@Benchmark
	public void batchWrites() {
		batch.clear();
		for (int i = 0; i < BATCH_SIZE; i++) {
			batch.put(batchKeys[i], BATCH_VALUE);
		}
		db.write(batch);
	}

	@Benchmark
	public void batchWritesArena() {
		batch.clear();
		try (Arena arena = Arena.ofConfined()) {
			for (int i = 0; i < BATCH_SIZE; i++) {
				batch.put(arena, batchKeys[i], BATCH_VALUE);
			}
			db.write(arena, batch);
		}
	}

	private static void deleteDir(Path dir) throws IOException {
		Files.walk(dir)
				.sorted(Comparator.reverseOrder())
				.forEach(p -> p.toFile().delete());
	}

	static void main() throws Exception {
		org.openjdk.jmh.runner.options.Options opt = new OptionsBuilder()
				.addProfiler(GCProfiler.class)
				.include(FfmBenchmark.class.getSimpleName())
				.build();

		new org.openjdk.jmh.runner.Runner(opt).run();
	}
}
