#!/usr/bin/env bash
# Run FFM and JNI benchmarks and print a side-by-side comparison.
# Usage: ./scripts/benchmark.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo ">>> Building..."
mvn install -DskipTests -q

echo ">>> Compiling benchmark sources..."
mvn -pl benchmarks test-compile -q

# Extract the full test-scope classpath (deps only, no Maven log noise)
DEPS=$(mvn -pl benchmarks dependency:build-classpath \
           -Dmdep.includeScope=test 2>/dev/null \
       | grep -v '^\[' | tr -d '[:space:]')

CP="benchmarks/target/test-classes:benchmarks/target/classes:core/target/classes:$DEPS"

echo ">>> Running benchmarks..."
java --enable-native-access=ALL-UNNAMED -cp "$CP" \
    io.github.dfa1.rocksdbffm.benchmark.BenchmarkRunner
