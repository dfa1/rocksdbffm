package io.github.dfa1.rocksdbffm.benchmark;

import io.github.dfa1.rocksdbffm.CopyResult;
import io.github.dfa1.rocksdbffm.ReadWriteDB;
import io.github.dfa1.rocksdbffm.RocksDB;
import io.github.dfa1.rocksdbffm.WriteBatch;
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
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsPrepend = {"--enable-native-access=ALL-UNNAMED", "--sun-misc-unsafe-memory-access=allow"})
public class FfmBenchmark {

	private ReadWriteDB db;
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

	// Instant tier — 8-byte long value, byte[] get + deserialize vs zero-copy get(key, Mapper)
	private byte[] instantKeyBytes;
	private MemorySegment instantKeyMemorySegment;

	// Blob tier — same byte[] get vs get(key, Mapper) comparison, swept across value sizes to
	// see whether the zero-copy gain grows with the amount of data that would otherwise
	// be copied. JMH requires @Param fields to be public.
	@Param({"8", "16", "1024", "4096", "65536", "1048576"})
	public int blobValueSize;
	private byte[] blobKeyBytes;
	private MemorySegment blobKeyMemorySegment;
	// Pre-allocated exactly once at trial size, reused across all invocations — models a
	// caller who already knows the value size and sizes their buffer accordingly.
	private MemorySegment blobValueMemorySegment;

	@Setup(Level.Trial)
	public void setup() throws Exception {
		dbPath = Files.createTempDirectory("bench-ffm-");
		db = RocksDB.open(dbPath);

		// --- byte[] tier ---
		writeKeyBytes = TestData.WRITE_KEY_BYTES.clone();
		writeValueBytes = TestData.WRITE_VALUE_BYTES.clone();
		readKeyBytes = TestData.READ_KEY_BYTES.clone();

		// --- ByteBuffer tier ---
		writeKeyByteBuffer = ByteBuffer.allocateDirect(TestData.WRITE_KEY_BYTES.length);
		writeKeyByteBuffer.put(TestData.WRITE_KEY_BYTES).flip();
		writeValByteBuffer = ByteBuffer.allocateDirect(TestData.WRITE_VALUE_BYTES.length);
		writeValByteBuffer.put(TestData.WRITE_VALUE_BYTES).flip();
		readKeyByteBuffer = ByteBuffer.allocateDirect(TestData.READ_KEY_BYTES.length);
		readKeyByteBuffer.put(TestData.READ_KEY_BYTES).flip();
		readValByteBuffer = ByteBuffer.allocateDirect(64);

		// --- MemorySegment tier ---
		arenaMemorySegment = Arena.ofConfined();
		writeKeyMemorySegment = arenaMemorySegment.allocateFrom(ValueLayout.JAVA_BYTE, TestData.WRITE_KEY_BYTES);
		writeValueMemorySegment = arenaMemorySegment.allocateFrom(ValueLayout.JAVA_BYTE, TestData.WRITE_VALUE_BYTES);
		readKeyMemorySegment = arenaMemorySegment.allocateFrom(ValueLayout.JAVA_BYTE, TestData.READ_KEY_BYTES);
		readValMemorySegment = arenaMemorySegment.allocate(64);

		// Seed the read key
		db.put(TestData.READ_KEY_BYTES, TestData.READ_VALUE_BYTES);

		// --- Instant tier ---
		instantKeyBytes = "instant-key".getBytes();
		instantKeyMemorySegment = arenaMemorySegment.allocateFrom(ValueLayout.JAVA_BYTE, instantKeyBytes);
		ByteBuffer instantValue = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.nativeOrder());
		instantValue.putLong(Instant.now().getEpochSecond());
		db.put(instantKeyBytes, instantValue.array());

		// --- blob tier ---
		blobKeyBytes = "blob-key".getBytes();
		blobKeyMemorySegment = arenaMemorySegment.allocateFrom(ValueLayout.JAVA_BYTE, blobKeyBytes);
		byte[] blobValue = new byte[blobValueSize];
		new Random(42).nextBytes(blobValue);
		db.put(blobKeyBytes, blobValue);
		blobValueMemorySegment = arenaMemorySegment.allocate(blobValueSize);

		// --- batch ---
		batchKeys = TestData.batchKeys();
		batch = WriteBatch.create();
	}

	@TearDown(Level.Trial)
	public void teardown() throws IOException {
		batch.close();
		db.close();
		arenaMemorySegment.close();
		TestData.deleteDir(dbPath);
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
	public CopyResult readsDirectByteBuffer() {
		readKeyByteBuffer.rewind();
		readValByteBuffer.clear();
		return db.get(readKeyByteBuffer, readValByteBuffer);
	}

	// ---- byte[] via rocksdb_get_into_buffer, for comparison against readsBytes ---------

	@Benchmark
	public byte[] getViaCopy() {
		readKeyByteBuffer.rewind();
		readValByteBuffer.clear();
		CopyResult result = db.get(readKeyByteBuffer, readValByteBuffer);
		if (!(result instanceof CopyResult.Copied)) {
			throw new IllegalStateException("unexpected: " + result);
		}
		readValByteBuffer.flip();
		byte[] bytes = new byte[readValByteBuffer.remaining()];
		readValByteBuffer.get(bytes);
		return bytes;
	}

	// ---- MemorySegment tier (FFM-only) ------------------------------------

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

	// ---- Instant deserialize: byte[] get vs zero-copy get(key, Mapper) (FFM-only) ----

	@Benchmark
	public Instant readsInstantViaByteArray() {
		byte[] value = db.get(instantKeyBytes);
		long epochSecond = ByteBuffer.wrap(value).order(ByteOrder.nativeOrder()).getLong();
		return Instant.ofEpochSecond(epochSecond);
	}

	@Benchmark
	public Instant readsInstantViaPinned() {
		return db.get(instantKeyMemorySegment,
				value -> Instant.ofEpochSecond(value.get(ValueLayout.JAVA_LONG, 0))).orElseThrow();
	}

	// ---- Blob read, value size swept via @Param: three tiers -----------------
	// byte[] get (allocates a new array every call), get(MemorySegment,MemorySegment)
	// (caller-preallocated buffer, still a native memcpy into it), and get(key, Mapper)
	// (zero-copy, no allocation and no copy at all).

	@Benchmark
	public int readsBlobViaByteArray() {
		return db.get(blobKeyBytes).length;
	}

	@Benchmark
	public long readsBlobViaMemorySegment() {
		return db.get(blobKeyMemorySegment, blobValueMemorySegment);
	}

	@Benchmark
	public long readsBlobViaPinned() {
		return db.get(blobKeyMemorySegment, MemorySegment::byteSize).orElseThrow();
	}

	// ---- batch (byte[] keys, same as JNI) ---------------------------------

	@Benchmark
	public void batchWrites() {
		batch.clear();
		for (int i = 0; i < TestData.WRITE_BATCH_SIZE; i++) {
			batch.put(batchKeys[i], TestData.BATCH_VALUE);
		}
		db.write(batch);
	}

	@Benchmark
	public void batchWritesArena() {
		batch.clear();
		try (Arena arena = Arena.ofConfined()) {
			for (int i = 0; i < TestData.WRITE_BATCH_SIZE; i++) {
				batch.put(arena, batchKeys[i], TestData.BATCH_VALUE);
			}
			db.write(arena, batch);
		}
	}

	static void main() throws Exception {
		org.openjdk.jmh.runner.options.Options opt = new OptionsBuilder()
				.addProfiler(GCProfiler.class)
				.include(FfmBenchmark.class.getSimpleName())
				.build();

		new org.openjdk.jmh.runner.Runner(opt).run();
	}
}
