package io.github.dfa1.rocksdbffm.benchmark;

import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Runs FFM and JNI benchmarks back-to-back and prints a comparison table.
 *
 * <pre>{@code
 * ./scripts/benchmark.sh
 * }</pre>
 *
 * <p>Tiers compared:
 * <ul>
 *   <li>byte[]       — FFM vs JNI</li>
 *   <li>ByteBuffer   — FFM vs JNI</li>
 *   <li>MemorySegment — FFM only (no JNI equivalent)</li>
 * </ul>
 */
public class BenchmarkRunner {

	// Display order drives LABELS iteration — ROW_ORDER is unused (LABELS is LinkedHashMap)

	// Human-readable labels
	private static final Map<String, String> LABELS = new LinkedHashMap<>();

	static {
		LABELS.put("readsBytes", "Read  — byte[]");
		LABELS.put("readsDirectByteBuffer", "Read  — DirectByteBuffer");
		LABELS.put("readsMemorySegment", "Read  — MemorySegment (FFM)");
		LABELS.put("writesBytes", "Write — byte[]");
		LABELS.put("writesDirectByteBuffer", "Write — DirectByteBuffer");
		LABELS.put("writesMemorySegment", "Write — MemorySegment (FFM)");
		LABELS.put("batchWrites", "Batch write (100 ops)");
	}

	public static void main(String[] args) throws Exception {
		// LinkedHashMap preserves insertion order (FFM benchmarks come first)
		Map<String, double[]> scores = new LinkedHashMap<>();

		run(FfmBenchmark.class, "FFM", scores, 0);
		run(JniBenchmark.class, "JNI", scores, 1);

		printTable(scores);
	}

	private static void run(Class<?> benchClass, String label, Map<String, double[]> scores, int col)
			throws Exception {
		System.out.printf("%n=== %s ===%n%n", label);
		Options opt = new OptionsBuilder()
				.include(benchClass.getSimpleName())
				.build();
		Collection<RunResult> results = new Runner(opt).run();
		for (RunResult r : results) {
			String name = r.getPrimaryResult().getLabel();
			scores.computeIfAbsent(name, k -> new double[2])[col] =
					r.getPrimaryResult().getStatistics().getMean();
		}
	}

	private static void printTable(Map<String, double[]> scores) {
		System.out.println();
		System.out.println("=".repeat(76));
		System.out.printf("%-32s %14s %14s %8s%n", "Benchmark", "FFM (ops/s)", "JNI (ops/s)", "Gain");
		System.out.println("-".repeat(76));

		// Print in defined order, skipping unknowns; append any remainder
		java.util.Set<String> printed = new java.util.LinkedHashSet<>();
		for (String key : LABELS.keySet()) {
			if (scores.containsKey(key)) {
				printRow(key, scores.get(key));
				printed.add(key);
			}
		}
		// Any benchmark not in LABELS (future additions)
		for (Map.Entry<String, double[]> e : scores.entrySet()) {
			if (!printed.contains(e.getKey())) {
				printRow(e.getKey(), e.getValue());
			}
		}
		System.out.println("=".repeat(76));
	}

	private static void printRow(String key, double[] vals) {
		String label = LABELS.getOrDefault(key, key);
		double ffm = vals[0];
		double jni = vals[1];
		boolean jniAvail = jni > 0;
		String jniStr = jniAvail ? String.format("%,14.0f", jni) : "           N/A";
		String gainStr = jniAvail ? String.format("%+7.1f%%", (ffm - jni) / jni * 100) : "    N/A";
		System.out.printf("%-32s %,14.0f %s %s%n", label, ffm, jniStr, gainStr);
	}

	private BenchmarkRunner() {
		// no instances
	}
}
