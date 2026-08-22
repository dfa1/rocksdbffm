package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/// Regression test for the `System.exit()` deadlock described on [BackgroundUpcallThreads]: once
/// an [EventNotifier] callback has run, a RocksDB background thread is attached to the JVM, and
/// exiting used to wedge the process forever.
///
/// This has to run in a forked JVM — the failure mode is "the JVM never exits", which cannot be
/// observed from inside the JVM that fails to exit.
class EventNotifierExitTest {

	/// Generous: a healthy exit takes well under a second, and a regressed one never happens at
	/// all, so the exact bound only decides how long a failing build waits.
	private static final int EXIT_TIMEOUT_SECONDS = 60;

	/// Signals from [ExitProbe] that no flush callback ever fired, so no background thread was
	/// attached and a clean exit would not have proved anything.
	private static final int NO_CALLBACK_EXIT_CODE = 2;

	@Test
	void systemExit_completesAfterABackgroundThreadCallbackHasRun(@TempDir Path dir) throws Exception {
		// Given — a forked JVM that fires an EventNotifier callback and then calls System.exit(0)
		Process probe = startProbe(dir);

		// When
		boolean exited;
		try {
			exited = probe.waitFor(EXIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		} finally {
			probe.destroyForcibly();
		}

		// Then
		assertThat(exited)
				.as("forked JVM did not exit within %ds after an EventNotifier callback ran",
						EXIT_TIMEOUT_SECONDS)
				.isTrue();
		assertThat(probe.exitValue())
				.as("probe exit code (%d means no flush callback fired)", NO_CALLBACK_EXIT_CODE)
				.isZero();
	}

	private static Process startProbe(Path dir) throws IOException {
		List<String> command = new ArrayList<>();
		command.add(ProcessHandle.current().info().command()
				.orElseGet(() -> Path.of(System.getProperty("java.home"), "bin", "java").toString()));
		command.add("--enable-native-access=ALL-UNNAMED");
		command.add("-cp");
		command.add(System.getProperty("java.class.path"));
		command.add(ExitProbe.class.getName());
		command.add(dir.toString());
		return new ProcessBuilder(command)
				.redirectErrorStream(true)
				.redirectOutput(ProcessBuilder.Redirect.INHERIT)
				.start();
	}

	/// Runs in the forked JVM: opens a DB with an [EventNotifier] attached, flushes so a
	/// background thread actually calls back into Java, then exits via `System.exit`.
	static final class ExitProbe {

		private ExitProbe() {
		}

		/// Entry point for the forked JVM.
		///
		/// @param args a single element: the directory to open the database in
		public static void main(String[] args) {
			AtomicInteger callbacks = new AtomicInteger();
			EventNotifier notifier = new EventNotifier() {
				@Override
				public void onFlushCompleted(FlushJobInfo info) {
					callbacks.incrementAndGet();
				}
			};

			try (var opts = Options.newOptions().setCreateIfMissing(true).addEventListener(notifier);
			     var db = RocksDB.openReadWrite(opts, Path.of(args[0]))) {
				db.put("k".getBytes(), "v".getBytes());
				db.flush(FlushOptions.newFlushOptions());
			}

			System.exit(callbacks.get() > 0 ? 0 : NO_CALLBACK_EXIT_CODE);
		}
	}
}
