package io.github.dfa1.rocksdbffm;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/// Infrastructure — library loading and symbol lookup
public class NativeLibrary {

	private static final Linker LINKER = Linker.nativeLinker();
	private static final SymbolLookup LIB = SymbolLookup.libraryLookup(resolveLibPath(), Arena.ofAuto());

	static MethodHandle lookup(String name, FunctionDescriptor fd, Linker.Option... options) {
		return LINKER.downcallHandle(
				LIB.find(name).orElseThrow(() ->
						new UnsatisfiedLinkError("Symbol not found: " + name)),
				fd, options);
	}

	private static String resolveLibPath() {
		String explicit = System.getProperty("rocksdb.lib.path");
		if (explicit != null) {
			return explicit;
		}

		String classifier = classifier();
		String ext = classifier.startsWith("osx") ? "dylib" : classifier.startsWith("windows") ? "dll" : "so";
		String resource = "/native/" + classifier + "/librocksdb." + ext;

		try (InputStream in = RocksDB.class.getResourceAsStream(resource)) {
			if (in == null) {
				throw new UnsatisfiedLinkError("No bundled RocksDB library found for platform " + classifier);
			}
			return extract(in.readAllBytes(), ext);
		} catch (IOException e) {
			throw new UnsatisfiedLinkError("Failed to extract bundled RocksDB: " + e.getMessage());
		}
	}

	/// Extracts to a content-addressed path under the system temp directory
	/// instead of a fresh `Files.createTempFile` + `deleteOnExit()` per run: on
	/// Windows, a loaded DLL stays locked for the life of the process, so
	/// `deleteOnExit()` silently fails and every run leaks a copy. Hashing the
	/// bytes means repeated runs of the same version reuse the same file (no
	/// copy, nothing to delete), while an upgrade gets a fresh path instead of
	/// colliding with a copy a concurrently running older process still holds
	/// open.
	private static String extract(byte[] bytes, String ext) throws IOException {
		Path target = Path.of(System.getProperty("java.io.tmpdir"), "rocksdbffm-" + sha256(bytes) + "." + ext);
		if (Files.isRegularFile(target) && Files.size(target) == bytes.length) {
			return target.toString();
		}

		Path staging = Files.createTempFile(target.getParent(), "librocksdb-", "." + ext);
		try {
			Files.write(staging, bytes);
			try {
				Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
			} catch (IOException e) {
				// Another process already extracted (and is holding open) the same
				// content-addressed path; its content is byte-for-byte identical by
				// construction, so falling back to it is safe.
				if (!Files.isRegularFile(target)) {
					throw e;
				}
			}
		} finally {
			Files.deleteIfExists(staging);
		}
		return target.toString();
	}

	private static String sha256(byte[] bytes) {
		try {
			return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 not available", e);
		}
	}

	private static String classifier() {
		String os = System.getProperty("os.name", "").toLowerCase();
		String arch = System.getProperty("os.arch", "").toLowerCase();
		String osName = os.contains("mac") ? "osx" : os.contains("win") ? "windows" : "linux";
		String archName = (arch.equals("aarch64") || arch.equals("arm64")) ? "aarch64" : "x86_64";
		return osName + "-" + archName;
	}

	private NativeLibrary() {
		// no instances
	}
}
