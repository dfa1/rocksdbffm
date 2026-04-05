package com.example.bench;

import com.example.ffm.RocksDB;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
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
    private static final byte[] WRITE_KEY = "bench-key".getBytes();
    private static final byte[] WRITE_VALUE = "bench-value-data-0123456789".getBytes();

    private RocksDB db;
    private Path dbPath;
    private byte[][] readKeys;

    @State(Scope.Thread)
    public static class Counter {
        int index = 0;
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        dbPath = Files.createTempDirectory("bench-ffm-");
        db = RocksDB.open(dbPath.toString());

        readKeys = new byte[NUM_KEYS][];
        byte[] value = "read-value-data-0123456789".getBytes();
        for (int i = 0; i < NUM_KEYS; i++) {
            readKeys[i] = ("key-" + i).getBytes();
            db.put(readKeys[i], value);
        }
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException {
        db.close();
        deleteDir(dbPath);
    }

    @Benchmark
    public void writes() {
        db.put(WRITE_KEY, WRITE_VALUE);
    }

    @Benchmark
    public byte[] reads(Counter counter) {
        return db.get(readKeys[counter.index++ % NUM_KEYS]);
    }

    private static void deleteDir(Path dir) throws IOException {
        Files.walk(dir)
             .sorted(Comparator.reverseOrder())
             .forEach(p -> p.toFile().delete());
    }
}
