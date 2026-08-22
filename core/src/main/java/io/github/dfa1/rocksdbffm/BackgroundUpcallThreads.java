package io.github.dfa1.rocksdbffm;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.atomic.AtomicBoolean;

/// Keeps `System.exit()` from deadlocking once a RocksDB background thread has been attached to
/// the JVM by one of this library's upcalls ([EventNotifier], [Logger], [MergeOperator.Custom]).
///
/// The deadlock is a three-way cycle between HotSpot, libc and RocksDB, and it is deterministic —
/// not a race:
///
/// 1. A Java thread calls `System.exit()`. HotSpot brings the world to a safepoint, sets its
///    global `_vm_exited` flag while holding `Threads_lock`, and — from the VM thread — calls
///    libc `exit()`.
/// 2. `exit()` runs static destructors, which include RocksDB's `PosixEnv::JoinThreadsOnExit`.
///    That destructor tells every default-`Env` thread pool to stop and then `pthread_join`s each
///    background thread.
/// 3. A background thread that once ran an upcall is an *attached* JVM thread. As it unwinds,
///    its thread-local destructor runs the JDK's `UpcallContext::~UpcallContext` →
///    `DetachCurrentThread` → `VM_Exit::wait_if_vm_exited`, which — by design — blocks forever on
///    the `Threads_lock` the exiting VM thread never releases, because the process is expected to
///    die momentarily.
///
/// It never dies: step 2 is waiting on the thread that step 3 has parked. Only threads that ran
/// an upcall are affected, which is why plain RocksDB use exits fine.
///
/// Step 3 is worth flagging as load-bearing but unspecified. Neither `java.lang.foreign.Linker`
/// nor dev.java's upcall tutorial says anything about threads: the javadoc's only documented
/// upcall hazards are an exception escaping the target handle and a function-pointer type
/// mismatch. That an unknown native thread gets attached on its first upcall — and detached from
/// a thread-local destructor when it dies — is HotSpot implementation behavior
/// (`UpcallLinker::on_entry`), observed here in a native stack trace, not a contract. A future
/// JDK could change it, at which point this whole class becomes dead weight rather than wrong.
/// The regression test that would notice either way forks a JVM and asserts that it exits.
///
/// The fix is to get those threads to exit while the JVM is still fully alive, so their detach
/// completes normally. Shrinking a pool to zero makes each excess thread `detach()` itself and
/// drop out of the pool's `bgthreads_` vector, so by the time `JoinThreadsOnExit` runs there is
/// nothing left for it to join. A shutdown hook is the last point where that is still possible.
final class BackgroundUpcallThreads {

	/// Upper bound on how long [#drain()] waits for the background threads to go away. Draining
	/// normally takes well under a millisecond; the bound only matters if a thread is wedged for
	/// some unrelated reason, and timing out is no worse than not having the hook at all.
	private static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(5);

	/// Threads RocksDB called one of our upcalls on. Identity-based (`Thread` does not override
	/// `equals`), so there is one entry per thread, not one per callback.
	private static final KeySetView<Thread, Boolean> THREADS = ConcurrentHashMap.newKeySet();

	/// Size past which [#track()] sweeps dead threads out of [#THREADS].
	///
	/// Pool threads are created once and live for the process, but they are not the only ones that
	/// reach here: a merge operator's callbacks also run on the short-lived subcompaction threads
	/// RocksDB spawns per compaction and joins when it finishes. Without a sweep those would
	/// accumulate one dead `Thread` reference per compaction, forever.
	private static final int PRUNE_THRESHOLD = 64;

	private static final AtomicBoolean HOOK_INSTALLED = new AtomicBoolean();

	private BackgroundUpcallThreads() {
	}

	/// Installs the shutdown hook that drains RocksDB's background threads, unless it is already
	/// installed. Called when a callback is registered rather than when one fires, so that the
	/// hook is set up on an ordinary Java thread instead of from inside a native upcall.
	static void installShutdownDrain() {
		if (HOOK_INSTALLED.compareAndSet(false, true)) {
			try {
				Runtime.getRuntime().addShutdownHook(
						new Thread(BackgroundUpcallThreads::drain, "rocksdbffm-background-thread-drain"));
			} catch (IllegalStateException alreadyShuttingDown) {
				// Nothing left to arm: the JVM is already on its way out, so any callback
				// registered now will not outlive it.
			}
		}
	}

	/// Records that the calling thread has run an upcall, and is therefore an attached JVM thread
	/// that [#drain()] must wait for. Called from every upcall dispatch that RocksDB can invoke on
	/// a background thread, including hot ones like a merge operator's `full_merge`.
	///
	/// The common case — a pool thread that is already tracked — is one `ConcurrentHashMap` lookup
	/// that finds the entry and writes nothing. Sweeping is amortized onto the rare call that adds
	/// a thread, so a callback firing repeatedly on the same thread never pays for it.
	static void track() {
		if (THREADS.add(Thread.currentThread()) && THREADS.size() > PRUNE_THRESHOLD) {
			THREADS.removeIf(thread -> !thread.isAlive());
		}
	}

	/// Shrinks the default `Env`'s thread pools to zero and waits for the tracked threads to
	/// terminate — and with them, to detach from the JVM.
	///
	/// Only the low- and high-priority pools are shrunk: they are the only ones this library can
	/// populate. RocksDB's bottom-priority pool defaults to zero threads and there is no API here
	/// to grow it, and subcompaction threads are joined by the compaction that spawned them, long
	/// before the JVM starts exiting.
	///
	/// This does stop background flushes and compactions for any database still open at shutdown,
	/// including work already queued. That is a deliberate trade: shutdown hooks run concurrently
	/// with no ordering guarantee, so a database still open here is one the application never
	/// closed, and its queued background work was not going to survive the exit either way.
	private static void drain() {
		try (Env env = Env.defaultEnv()) {
			env.setBackgroundThreads(0);
			env.setHighPriorityBackgroundThreads(0);
		}
		awaitTermination(THREADS);
	}

	private static void awaitTermination(Set<Thread> threads) {
		long deadline = System.nanoTime() + DRAIN_TIMEOUT.toNanos();
		for (Thread thread : threads) {
			long remaining = deadline - System.nanoTime();
			if (remaining <= 0) {
				return;
			}
			try {
				thread.join(Duration.ofNanos(remaining));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}
}
