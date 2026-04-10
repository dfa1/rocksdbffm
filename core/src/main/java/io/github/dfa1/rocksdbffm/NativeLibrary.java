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

/// Infrastructure — library loading and symbol lookup
public class NativeLibrary {

	private static final Linker LINKER = Linker.nativeLinker();
	private static final SymbolLookup LIB = SymbolLookup.libraryLookup(resolveLibPath(), Arena.ofAuto());

	static MethodHandle lookup(String name, FunctionDescriptor fd) {
		return LINKER.downcallHandle(
				LIB.find(name).orElseThrow(() ->
						new UnsatisfiedLinkError("Symbol not found: " + name)),
				fd);
	}

	private static String resolveLibPath() {
		String explicit = System.getProperty("rocksdb.lib.path");
		if (explicit != null) {
			return explicit;
		}

		String classifier = classifier();
		String ext = classifier.startsWith("osx") ? "dylib" : "so";
		String resource = "/native/" + classifier + "/librocksdb." + ext;

		try (InputStream in = RocksDB.class.getResourceAsStream(resource)) {
			if (in == null) {
				throw new UnsatisfiedLinkError("No bundled RocksDB library found for platform " + classifier);
			}
			Path tmp = Files.createTempFile("librocksdb-", "." + ext);
			tmp.toFile().deleteOnExit();
			Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
			return tmp.toString();
		} catch (IOException e) {
			throw new UnsatisfiedLinkError("Failed to extract bundled RocksDB: " + e.getMessage());
		}
	}

	private static String classifier() {
		String os = System.getProperty("os.name", "").toLowerCase();
		String arch = System.getProperty("os.arch", "").toLowerCase();
		String osName = os.contains("mac") ? "osx" : "linux";
		String archName = (arch.equals("aarch64") || arch.equals("arm64")) ? "aarch64" : "x86_64";
		return osName + "-" + archName;
	}

	private NativeLibrary() {
		// no instances
	}
}
