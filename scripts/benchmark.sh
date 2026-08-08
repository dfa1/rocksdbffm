#!/usr/bin/env bash
# Run FFM and JNI benchmarks and print a side-by-side comparison.
#
# Usage:
#   ./scripts/benchmark.sh                     # the FFM-vs-JNI comparison table
#   ./scripts/benchmark.sh PinnedReadBenchmark # a single benchmark class
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

MAIN_CLASS="io.github.dfa1.rocksdbffm.benchmark.${1:-BenchmarkRunner}"

# One reactor invocation builds every upstream module and resolves the test-scope
# classpath from it. Inside a single session Maven resolves sibling modules to their
# target/classes, so nothing has to be installed into ~/.m2 first.
#
# mdep.outputFile is deliberately relative: each module writes its own file into its
# own target/, so there is no race over a shared path and we can read exactly the one
# belonging to benchmarks.
echo ">>> Building and resolving classpath..."
./mvnw -q -pl benchmarks -am test-compile \
    dependency:build-classpath \
    -Dmdep.includeScope=test \
    -Dmdep.outputFile=target/benchmark-classpath.txt

DEPS="$(cat benchmarks/target/benchmark-classpath.txt)"
CP="benchmarks/target/test-classes:benchmarks/target/classes:$DEPS"

echo ">>> Running ${MAIN_CLASS}..."
java --enable-native-access=ALL-UNNAMED \
    --sun-misc-unsafe-memory-access=allow \
    -cp "$CP" \
    "$MAIN_CLASS"
