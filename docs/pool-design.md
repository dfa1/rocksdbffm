We were designing a Java FFM pool abstraction. Current state:

- Interface: Pool<T> extends AutoCloseable with acquire(), release(), capacity(), available()
- BlockingPool<T>: generic impl using LinkedBlockingQueue(capacity), fail-fast on exhaustion and double-release
- CharPtrPtrPool implements Pool<MemorySegment>: owns Arena.ofShared(), delegates to BlockingPool, zeroes slot on release, contiguous block allocation via asSlice
- UnpooledPool: test stub, allocates per-call from Arena.ofShared(), no-op release

Open questions:
- Stateful anonymous Supplier in BlockingPool constructor is ugly, consider factory method
- BlockingPool.close() is a no-op generically — caller must close CharPtrPtrPool explicitly

Important points:
- clear on acquire() not non release()
-    clients of the pool will receive always a well known state