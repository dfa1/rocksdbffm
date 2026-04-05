/**
 * FFM-based RocksDB wrapper for JDK 25+.
 *
 * <p>Uses {@code java.lang.foreign} (part of {@code java.base}) to call
 * the native RocksDB C library directly — no JNI required.
 *
 * <p>The JVM must grant native access to this module:
 * <pre>{@code
 *   --enable-native-access=io.github.dfa1.rocksdbffm
 * }</pre>
 */
module io.github.dfa1.rocksdbffm {
    exports io.github.dfa1.rocksdbffm;
}
