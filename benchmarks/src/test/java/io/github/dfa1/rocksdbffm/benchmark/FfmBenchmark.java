package io.github.dfa1.rocksdbffm.benchmark;

import io.github.dfa1.rocksdbffm.RocksDB;
import io.github.dfa1.rocksdbffm.WriteBatch;
import org.openjdk.jmh.annotations.*;

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

    private static final int BATCH_SIZE = 100;
    private static final byte[] READ_KEY_BYTES  = "read-key".getBytes();
    private static final byte[] READ_VALUE_BYTES = "read-value-data-0123456789".getBytes();
    private static final byte[] WRITE_KEY_BYTES  = "bench-key".getBytes();
    private static final byte[] WRITE_VALUE_BYTES = "bench-value-data-0123456789".getBytes();
    private static final byte[] BATCH_VALUE = "batch-value-data-0123456789".getBytes();

    private RocksDB db;
    private Path dbPath;

    // byte[] tier
    private byte[] writeKey;
    private byte[] writeValue;
    private byte[] readKey;

    // ByteBuffer tier
    private ByteBuffer writeKeyBuf;
    private ByteBuffer writeValBuf;
    private ByteBuffer readKeyBuf;
    private ByteBuffer readValBuf;

    // MemorySegment tier — backed by an auto arena for the benchmark lifetime
    private Arena msArena;
    private MemorySegment writeKeyMs;
    private MemorySegment writeValMs;
    private MemorySegment readKeyMs;
    private MemorySegment readValMs;

    private WriteBatch batch;
    private byte[][] batchKeys;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        dbPath = Files.createTempDirectory("bench-ffm-");
        db = RocksDB.open(dbPath);

        // --- byte[] tier ---
        writeKey = WRITE_KEY_BYTES.clone();
        writeValue = WRITE_VALUE_BYTES.clone();
        readKey = READ_KEY_BYTES.clone();

        // --- ByteBuffer tier ---
        writeKeyBuf = ByteBuffer.allocateDirect(WRITE_KEY_BYTES.length);
        writeKeyBuf.put(WRITE_KEY_BYTES).flip();
        writeValBuf = ByteBuffer.allocateDirect(WRITE_VALUE_BYTES.length);
        writeValBuf.put(WRITE_VALUE_BYTES).flip();
        readKeyBuf = ByteBuffer.allocateDirect(READ_KEY_BYTES.length);
        readKeyBuf.put(READ_KEY_BYTES).flip();
        readValBuf = ByteBuffer.allocateDirect(64);

        // --- MemorySegment tier ---
        msArena = Arena.ofAuto();
        writeKeyMs = msArena.allocateFrom(ValueLayout.JAVA_BYTE, WRITE_KEY_BYTES);
        writeValMs = msArena.allocateFrom(ValueLayout.JAVA_BYTE, WRITE_VALUE_BYTES);
        readKeyMs = msArena.allocateFrom(ValueLayout.JAVA_BYTE, READ_KEY_BYTES);
        readValMs = msArena.allocate(64);

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
        deleteDir(dbPath);
    }

    // ---- byte[] tier -------------------------------------------------------

    @Benchmark
    public void writesBytes() {
        db.put(writeKey, writeValue);
    }

    @Benchmark
    public byte[] readsBytes() {
        return db.get(readKey);
    }

    // ---- ByteBuffer tier ---------------------------------------------------

    @Benchmark
    public void writesDirectByteBuffer() {
        writeKeyBuf.rewind();
        writeValBuf.rewind();
        db.put(writeKeyBuf, writeValBuf);
    }

    @Benchmark
    public int readsDirectByteBuffer() {
        readKeyBuf.rewind();
        readValBuf.clear();
        return db.get(readKeyBuf, readValBuf);
    }

    // ---- MemorySegment tier (FFM-only) ------------------------------------

    @Benchmark
    public void writesMemorySegment() {
        db.put(writeKeyMs, writeValMs);
    }

    @Benchmark
    public long readsMemorySegment() {
        return db.get(readKeyMs, readValMs);
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

    private static void deleteDir(Path dir) throws IOException {
        Files.walk(dir)
             .sorted(Comparator.reverseOrder())
             .forEach(p -> p.toFile().delete());
    }

    static void main() throws Exception {
        org.openjdk.jmh.runner.options.Options opt = new org.openjdk.jmh.runner.options.OptionsBuilder()
                .include(FfmBenchmark.class.getSimpleName())
                .build();

        new org.openjdk.jmh.runner.Runner(opt).run();
    }
}
