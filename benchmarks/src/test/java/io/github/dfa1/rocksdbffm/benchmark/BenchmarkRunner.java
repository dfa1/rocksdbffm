package io.github.dfa1.rocksdbffm.benchmark;

import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * Runs FFM and JNI benchmarks back-to-back and prints a comparison table.
 *
 * <pre>{@code
 * mvn -pl benchmarks exec:java
 * }</pre>
 */
public class BenchmarkRunner {

    public static void main(String[] args) throws Exception {
        Map<String, double[]> scores = new TreeMap<>();

        run(FfmBenchmark.class, "FFM", scores);
        run(JniBenchmark.class, "JNI", scores);

        printTable(scores);
    }

    private static void run(Class<?> benchClass, String label, Map<String, double[]> scores)
            throws Exception {
        System.out.printf("%n=== %s ===%n%n", label);
        Options opt = new OptionsBuilder()
                .include(benchClass.getSimpleName())
                .build();
        Collection<RunResult> results = new Runner(opt).run();
        int col = label.equals("FFM") ? 0 : 1;
        for (RunResult r : results) {
            String name = r.getPrimaryResult().getLabel();
            scores.computeIfAbsent(name, k -> new double[2])[col] =
                    r.getPrimaryResult().getStatistics().getMean();
        }
    }

    private static void printTable(Map<String, double[]> scores) {
        System.out.println();
        System.out.println("=".repeat(72));
        System.out.printf("%-24s %14s %14s %8s%n", "Benchmark", "FFM (ops/s)", "JNI (ops/s)", "Gain");
        System.out.println("-".repeat(72));
        for (Map.Entry<String, double[]> e : scores.entrySet()) {
            double ffm = e.getValue()[0];
            double jni = e.getValue()[1];
            double gain = (ffm - jni) / jni * 100;
            System.out.printf("%-24s %,14.0f %,14.0f %+7.1f%%%n",
                    e.getKey(), ffm, jni, gain);
        }
        System.out.println("=".repeat(72));
    }
}
