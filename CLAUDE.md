Project
----

This is an experimental wrapper for rocksdb using FFM and JDK 25.
Why we are building this? Because there is always a lag between cool stuff happening
in RocksDB C++ and the JNI wrappers. Lately the lag is getting big...

Package and module is io.github.dfa1.rocksdbffm.

How to link to native code
-----

By now, reuse the local installation of rocksdb via brew.

Use the files under:
/opt/homebrew/Cellar/rocksdb/${version}

jextract is not bundled with the JDK — bindings are written manually using
java.lang.foreign. The C header is at include/rocksdb/c.h.

*Technical debt*: have the build of rocksdb in the tree so it works for every
architecture.

How to validate
---

For every feature, build it with rocksdbjni and then rebuild it with FFM.
Write unit tests in JUnit 5 and @TempDir. JMH is used for all performance tests.

Run tests:      mvn test
Run benchmarks: mvn package -DskipTests && java --enable-native-access=ALL-UNNAMED -jar target/benchmarks.jar

What features to cover
----

- [x] create/open a rocksdb database
- [x] put/get/delete
- [x] batch
- [x] transaction
- [x] createIfMissing/readOnly
- [ ] iterator
- [ ] options
- [ ] statistics
- [ ] tracing
– [ ] secondary keys
- [ ] `ByteBuffer` pool and some examples
- [ ] `MemorySegment` version
- [ ] checkpoint
- [ ] module-info.java and full module support
- any other feature users will request


in case of doubt, use same design of RocksdbJNI (same names).

Use `java.nio.file.Path` wherever RocksDB accepts a file-system path (open, backup, checkpoint, etc.).
Never use raw `String` for paths in the FFM API surface.

Design is:
- have byte[] version of every method for quick access: explicitly mention that is slow in the javadoc
- have always the ByteBuffer version to support existing clients using it
- have also the MemorySegment version for new clients
- every feature is covered by unit tests, javadoc and examples

FFM Best Practices
----

Learned from:
- https://rocksdb.org/blog/2024/02/20/foreign-function-interface.html
- https://rocksdb.org/blog/2023/11/06/java-jni-benchmarks.html

### 1. Reuse thread-local auxiliary segments

Never allocate `errHolder` or `valLenHolder` on every call with `Arena.ofConfined()`.
Instead, pre-allocate them once per thread using `Arena.ofAuto()` via `ThreadLocal`:

```java
private static final ThreadLocal<MemorySegment> ERR_HOLDER = ThreadLocal.withInitial(
    () -> Arena.ofAuto().allocate(ValueLayout.ADDRESS));
```

Reset to NULL before each call, check after. Zero allocation on the hot path.

### 2. PinnableSlice for reads

Use `rocksdb_get_pinned` instead of `rocksdb_get`. The difference:

- `rocksdb_get`: DB::Get() → copies into `std::string` → malloc's a C string → caller frees
- `rocksdb_get_pinned`: DB::Get() → pins data directly from block cache → caller reads → destroy pin

The pinnable slice avoids the intermediate `std::string` copy entirely when the value
is in the block cache. Use `rocksdb_pinnableslice_value` to get the pointer + length,
copy to the destination, then `rocksdb_pinnableslice_destroy`.

### 3. Zero-copy puts with direct ByteBuffer

`MemorySegment.ofBuffer(directByteBuffer)` wraps the buffer's existing native memory —
no heap→native copy occurs. Combined with thread-local errHolder, the entire put path
has zero allocations.

### 4. Single-copy gets: PinnableSlice + direct output ByteBuffer

The optimal get path combines both techniques:
1. `rocksdb_get_pinned` pins from block cache (no intermediate copy)
2. `MemorySegment.ofBuffer(outputBuffer)` writes directly into the caller's buffer
3. One copy total: block cache → caller's buffer

### 5. Client-supplied buffers are the highest performance tier

API tiers (fastest to slowest):
1. `put(ByteBuffer, ByteBuffer)` / `get(ByteBuffer, ByteBuffer)` — zero alloc on hot path
2. `put(byte[], byte[])` / `get(byte[])` — Arena per call for key/value copy
3. Per-call allocation — avoid in tight loops

### Benchmark results (JDK 25, Apple M-series, single thread)

#### byte[] API
| Benchmark   |     ops/s |
|-------------|-----------|
| FFM reads   | 2,754,413 |
| JNI reads   | 2,107,679 |
| FFM writes  |   656,592 |
| JNI writes  |   603,817 |

#### direct ByteBuffer API (no optimizations)
| Benchmark   |     ops/s | vs byte[] |
|-------------|-----------|-----------|
| FFM reads   | 3,022,630 |     +10%  |
| JNI reads   | 2,042,639 |      -3%  |
| FFM writes  |   685,907 |      +4%  |
| JNI writes  |   596,238 |      -1%  |

#### direct ByteBuffer + PinnableSlice + thread-local arena
| Benchmark   |     ops/s | vs prev FFM | vs JNI |
|-------------|-----------|-------------|--------|
| FFM reads   | 3,372,008 |       +11%  |  +74%  |
| JNI reads   | 1,932,697 |        n/a  |        |
| FFM writes  |   677,159 |        -1%  |  +17%  |
| JNI writes  |   580,864 |        n/a  |        |

Reads benefit most: PinnableSlice eliminates the `std::string` intermediate copy,
giving a cumulative **+74% over JNI** on reads. Writes are bound by WAL/memtable
so the gains are smaller but consistent.
