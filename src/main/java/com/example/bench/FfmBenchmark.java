package com.example.bench;

import com.example.ffm.RocksDB;
import com.example.ffm.WriteBatch;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
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
@Fork(value = 1, jvmArgsPrepend = "--enable-native-access=ALL-UNNAMED")
public class FfmBenchmark {

    private static final int NUM_KEYS = 10_000;
    private static final int BATCH_SIZE = 100;
    private static final byte[] WRITE_KEY = "bench-key".getBytes();
    private static final byte[] WRITE_VALUE = "bench-value-data-0123456789".getBytes();
    private static final byte[] BATCH_VALUE = "batch-value-data-0123456789".getBytes();

    private RocksDB db;
    private Path dbPath;

    private ByteBuffer writeKeyBuf;
    private ByteBuffer writeValBuf;
    private ByteBuffer[] readKeyBufs;
    private ByteBuffer readValBuf;
    private WriteBatch batch;
    private byte[][] batchKeys;

    @State(Scope.Thread)
    public static class Counter {
        int index = 0;
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        dbPath = Files.createTempDirectory("bench-ffm-");
        db = RocksDB.open(dbPath.toString());

        writeKeyBuf = ByteBuffer.allocateDirect(WRITE_KEY.length);
        writeKeyBuf.put(WRITE_KEY).flip();
        writeValBuf = ByteBuffer.allocateDirect(WRITE_VALUE.length);
        writeValBuf.put(WRITE_VALUE).flip();

        readKeyBufs = new ByteBuffer[NUM_KEYS];
        readValBuf = ByteBuffer.allocateDirect(64);
        byte[] value = "read-value-data-0123456789".getBytes();
        for (int i = 0; i < NUM_KEYS; i++) {
            byte[] k = ("key-" + i).getBytes();
            readKeyBufs[i] = ByteBuffer.allocateDirect(k.length);
            readKeyBufs[i].put(k).flip();
            db.put(k, value);
        }

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

    @Benchmark
    public void writes() {
        writeKeyBuf.rewind();
        writeValBuf.rewind();
        db.put(writeKeyBuf, writeValBuf);
    }

    @Benchmark
    public int reads(Counter counter) {
        ByteBuffer key = readKeyBufs[counter.index++ % NUM_KEYS];
        key.rewind();
        readValBuf.clear();
        return db.get(key, readValBuf);
    }

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
}
