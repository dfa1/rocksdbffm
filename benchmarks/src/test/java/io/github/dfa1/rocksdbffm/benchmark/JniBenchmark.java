package io.github.dfa1.rocksdbffm.benchmark;

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
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsPrepend = {"--enable-native-access=ALL-UNNAMED", "--sun-misc-unsafe-memory-access=allow"})
public class JniBenchmark {

	static {
		RocksDB.loadLibrary();
	}

	private RocksDB db;
	private Options options;
	private WriteOptions writeOptions;
	private ReadOptions readOptions;
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

	private WriteBatch batch;
	private byte[][] batchKeys;

	@Setup(Level.Trial)
	public void setup() throws Exception {
		dbPath = Files.createTempDirectory("bench-jni-");
		options = new Options().setCreateIfMissing(true);
		db = RocksDB.open(options, dbPath.toString());
		writeOptions = new WriteOptions();
		readOptions = new ReadOptions();

		// --- byte[] tier ---
		writeKey = TestData.WRITE_KEY_BYTES.clone();
		writeValue = TestData.WRITE_VALUE_BYTES.clone();
		readKey = TestData.READ_KEY_BYTES.clone();

		// --- ByteBuffer tier ---
		writeKeyBuf = ByteBuffer.allocateDirect(TestData.WRITE_KEY_BYTES.length);
		writeKeyBuf.put(TestData.WRITE_KEY_BYTES).flip();
		writeValBuf = ByteBuffer.allocateDirect(TestData.WRITE_VALUE_BYTES.length);
		writeValBuf.put(TestData.WRITE_VALUE_BYTES).flip();
		readKeyBuf = ByteBuffer.allocateDirect(TestData.READ_KEY_BYTES.length);
		readKeyBuf.put(TestData.READ_KEY_BYTES).flip();
		readValBuf = ByteBuffer.allocateDirect(64);

		// Seed the read key
		db.put(TestData.READ_KEY_BYTES, TestData.READ_VALUE_BYTES);

		batchKeys = TestData.batchKeys();
		batch = new WriteBatch();
	}

	@TearDown(Level.Trial)
	public void teardown() throws Exception {
		batch.close();
		db.close();
		options.close();
		writeOptions.close();
		readOptions.close();
		TestData.deleteDir(dbPath);
	}

	// ---- byte[] tier -------------------------------------------------------

	@Benchmark
	public void writesBytes() throws Exception {
		db.put(writeKey, writeValue);
	}

	@Benchmark
	public byte[] readsBytes() throws Exception {
		return db.get(readKey);
	}

	// ---- ByteBuffer tier ---------------------------------------------------

	@Benchmark
	public void writesDirectByteBuffer() throws Exception {
		writeKeyBuf.rewind();
		writeValBuf.rewind();
		db.put(writeOptions, writeKeyBuf, writeValBuf);
	}

	@Benchmark
	public int readsDirectByteBuffer() throws Exception {
		readKeyBuf.rewind();
		readValBuf.clear();
		return db.get(readOptions, readKeyBuf, readValBuf);
	}

	// ---- batch (byte[] keys) -----------------------------------------------

	@Benchmark
	public void batchWrites() throws Exception {
		batch.clear();
		for (int i = 0; i < TestData.WRITE_BATCH_SIZE; i++) {
			batch.put(batchKeys[i], TestData.BATCH_VALUE);
		}
		db.write(writeOptions, batch);
	}

	static void main() throws Exception {
		org.openjdk.jmh.runner.options.Options opt = new org.openjdk.jmh.runner.options.OptionsBuilder()
				.include(JniBenchmark.class.getSimpleName())
				.build();

		new org.openjdk.jmh.runner.Runner(opt).run();
	}
}
