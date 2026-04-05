# AGENTS.md: Project Context & AI-Driven Guidelines

This file serves as the primary context for AI agents working on **rocksdbffm**. It contains the project's technical architecture, safety constraints, and performance-critical patterns.

## 🤖 AI-Driven Project Mandate
This project is heavily AI-driven. As an agent, your goal is to:
- **Be Autonomous:** Research C headers and identify the best mapping to Java FFM.
- **Stay Technical:** Prioritize performance, zero-copy, and manual memory safety.
- **Maintain Consistency:** Follow established naming and ownership patterns.

---

## 🛠 Tech Stack
- **Language:** Java 21+ (using JDK 25 features if available).
- **Core API:** `java.lang.foreign` (Foreign Function & Memory API).
- **Native Library:** RocksDB (C API via `include/rocksdb/c.h`).
- **Build System:** Maven.
- **Testing:** JUnit 5, AssertJ.
- **Benchmarking:** JMH (Java Microbenchmark Harness).

---

## 🏗 Architectural Standards

### 1. Manual Memory Management & Lifecycle
Every class wrapping a native pointer **must** implement `AutoCloseable`.
- **Zero Leaks:** Native resources must be destroyed in `close()`.
- **Ownership Transfer:** When one native object takes ownership of another (e.g., `FilterPolicy` → `BlockBasedTableConfig`), the transferring object must mark the ownership as transferred using a boolean flag. Its `close()` method should then become a no-op to prevent double-frees.
- **Transfer Marker:** Use a method like `transferOwnership()` inside the setter that takes ownership.

### 2. Data Types & Path Handling
To ensure type safety and consistent units across the API:
- **C API Only:** We use the RocksDB C interface (`rocksdb/c.h`). Do not attempt to link directly to C++ symbols.
- **Paths:** Never use raw `String` for file system paths. Always use `java.nio.file.Path` for any API surface that accepts paths (open, backup, checkpoint).
- **Memory Sizes:** Never use raw `long` for byte counts (e.g., cache size, write buffer size). Always use the project's `MemorySize` type.

### 3. API Surface Design
For every feature, provide three tiers of access:
1. **`MemorySegment` Version:** Native-first, for performance-critical usage.
2. **`ByteBuffer` Version:** For compatibility with existing NIO-based clients.
3. **`byte[]` Version:** Quick access for convenience (explicitly documented as slower).

---

## ⚡ FFM Performance & Patterns

### 1. Centralized Error Handling
**NEVER use ThreadLocals for error pointers.** Use the centralized `Native` utility:

```java
// Pattern A: Use caller's Arena (Preferred in hot paths)
try (Arena arena = Arena.ofConfined()) {
    MemorySegment err = Native.errHolder(arena);
    MH_DO_SOMETHING.invokeExact(handle, ..., err);
    Native.checkError(err);
}

// Pattern B: Use helper (Convenient for one-off calls)
Native.check(err -> MH_DO_SOMETHING.invokeExact(handle, ..., err));
```

### 2. Zero-Copy Patterns
- **PinnableSlice:** Use `rocksdb_get_pinned` for reads to avoid intermediate copies from the block cache.
- **Direct Buffers:** Use `MemorySegment.ofBuffer(directByteBuffer)` to wrap existing native memory without copies.

---

## 🧪 Validation & Workflow

### 1. Comparative Testing
For every new feature:
1. Compare the behavior against `rocksdbjni` if possible.
2. Write unit tests in JUnit 5 using `@TempDir`.
3. Follow the `// Given / // When / // Then` structure.

### 2. Benchmark First
Performance gains are a primary goal. Use `JMH` to validate changes.
- **Run tests:** `mvn test`
- **Run benchmarks:** `mvn package -DskipTests && java --enable-native-access=ALL-UNNAMED -jar target/benchmarks.jar`

---

## 🗺 Feature Map

| Feature | Status | Notes |
| :--- | :--- | :--- |
| DB Open/Create | ✅ Done | Options, CreateIfMissing, ReadOnly |
| Put/Get/Delete | ✅ Done | byte[], ByteBuffer, MemorySegment |
| WriteBatch | ✅ Done | |
| Transactions | ✅ Done | |
| Checkpoints | ✅ Done | |
| Table Options | ✅ Done | BlockBasedTableConfig, LRUCache, FilterPolicy |
| Iterators | ⏳ In Progress | |
| Statistics | ❌ Pending | |
| Tracing | ❌ Pending | |
| Secondary Keys | ❌ Pending | |
| Module Support | ⏳ In Progress | |

---

## 🏗 Technical Debt
- **Native Build Integration:** Currently relies on local `brew install rocksdb`. We need an in-tree build of RocksDB to support cross-architecture distribution.
