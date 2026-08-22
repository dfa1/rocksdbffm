package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ReadOptionsTest {

	@Test
	void setVerifyChecksums_roundTrips() {
		// Given
		try (var sut = ReadOptions.newReadOptions()) {

			// When
			sut.setVerifyChecksums(false);

			// Then
			assertThat(sut.isVerifyChecksums()).isFalse();
		}
	}

	@Test
	void setFillCache_roundTrips() {
		// Given
		try (var sut = ReadOptions.newReadOptions()) {

			// When
			sut.setFillCache(false);

			// Then
			assertThat(sut.isFillCache()).isFalse();
		}
	}

	@Test
	void setPinData_roundTrips() {
		// Given
		try (var sut = ReadOptions.newReadOptions()) {

			// When
			sut.setPinData(true);

			// Then
			assertThat(sut.isPinData()).isTrue();
		}
	}

	@Test
	void setTailing_roundTrips() {
		// Given
		try (var sut = ReadOptions.newReadOptions()) {

			// When
			sut.setTailing(true);

			// Then
			assertThat(sut.isTailing()).isTrue();
		}
	}

	@Test
	void setTotalOrderSeek_roundTrips() {
		// Given
		try (var sut = ReadOptions.newReadOptions()) {

			// When
			sut.setTotalOrderSeek(true);

			// Then
			assertThat(sut.isTotalOrderSeek()).isTrue();
		}
	}

	@Test
	void setPrefixSameAsStart_roundTrips() {
		// Given
		try (var sut = ReadOptions.newReadOptions()) {

			// When
			sut.setPrefixSameAsStart(true);

			// Then
			assertThat(sut.isPrefixSameAsStart()).isTrue();
		}
	}

	@Test
	void setReadaheadSize_roundTrips() {
		// Given
		try (var sut = ReadOptions.newReadOptions()) {

			// When
			sut.setReadaheadSize(MemorySize.ofKB(64));

			// Then
			assertThat(sut.getReadaheadSize()).isEqualTo(MemorySize.ofKB(64));
		}
	}

	@Test
	void setRequestId_roundTrips() {
		// Given
		try (var sut = ReadOptions.newReadOptions()) {

			// When
			sut.setRequestId("trace-123");

			// Then
			assertThat(sut.getRequestId()).contains("trace-123");
		}
	}

	@Test
	void setRequestId_null_clears() {
		// Given
		try (var sut = ReadOptions.newReadOptions()) {
			sut.setRequestId("trace-123");

			// When
			sut.setRequestId(null);

			// Then
			assertThat(sut.getRequestId()).isEmpty();
		}
	}

	@Test
	void getRequestId_unset_isEmpty() {
		// Given
		try (var sut = ReadOptions.newReadOptions()) {

			// When
			Optional<String> requestId = sut.getRequestId();

			// Then
			assertThat(requestId).isEmpty();
		}
	}

	@Test
	void defaults_matchRocksDbDefaults() {
		// Given
		try (var sut = ReadOptions.newReadOptions()) {

			// When
			// no action -- verifying defaults immediately after construction

			// Then
			assertThat(sut.isVerifyChecksums()).isTrue();
			assertThat(sut.isFillCache()).isTrue();
			assertThat(sut.isPinData()).isFalse();
			assertThat(sut.isTailing()).isFalse();
			assertThat(sut.isTotalOrderSeek()).isFalse();
			assertThat(sut.isPrefixSameAsStart()).isFalse();
			assertThat(sut.getReadaheadSize()).isEqualTo(MemorySize.ZERO);
			assertThat(sut.getRequestId()).isEmpty();
		}
	}

	// -----------------------------------------------------------------------
	// Iterate bounds -- no getter exists in the C API, so these are verified
	// through actual iteration behavior against a real DB.
	// -----------------------------------------------------------------------

	@Test
	void setIterateLowerBound_bytes_restrictsIteration(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openReadWrite(dir)) {
			db.put("a".getBytes(), "1".getBytes());
			db.put("b".getBytes(), "2".getBytes());
			db.put("c".getBytes(), "3".getBytes());

			try (var readOptions = ReadOptions.newReadOptions()) {
				readOptions.setIterateLowerBound("b".getBytes());

				// When
				List<String> keys = new ArrayList<>();
				try (RocksIterator it = db.newIterator(readOptions)) {
					for (it.seekToFirst(); it.isValid(); it.next()) {
						keys.add(new String(it.key()));
					}
					it.checkError();
				}

				// Then
				assertThat(keys).containsExactly("b", "c");
			}
		}
	}

	@Test
	void setIterateUpperBound_bytes_restrictsIteration(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openReadWrite(dir)) {
			db.put("a".getBytes(), "1".getBytes());
			db.put("b".getBytes(), "2".getBytes());
			db.put("c".getBytes(), "3".getBytes());

			try (var readOptions = ReadOptions.newReadOptions()) {
				readOptions.setIterateUpperBound("b".getBytes());

				// When
				List<String> keys = new ArrayList<>();
				try (RocksIterator it = db.newIterator(readOptions)) {
					for (it.seekToFirst(); it.isValid(); it.next()) {
						keys.add(new String(it.key()));
					}
					it.checkError();
				}

				// Then
				assertThat(keys).containsExactly("a");
			}
		}
	}

	@Test
	void setIterateLowerBound_memorySegment_restrictsIteration(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openReadWrite(dir);
		     var arena = Arena.ofConfined()) {
			db.put("a".getBytes(), "1".getBytes());
			db.put("b".getBytes(), "2".getBytes());
			db.put("c".getBytes(), "3".getBytes());

			try (var readOptions = ReadOptions.newReadOptions()) {
				MemorySegment bound = arena.allocateFrom("b", StandardCharsets.US_ASCII);
				readOptions.setIterateLowerBound(bound.asSlice(0, 1));

				// When
				List<String> keys = new ArrayList<>();
				try (RocksIterator it = db.newIterator(readOptions)) {
					for (it.seekToFirst(); it.isValid(); it.next()) {
						keys.add(new String(it.key()));
					}
					it.checkError();
				}

				// Then
				assertThat(keys).containsExactly("b", "c");
			}
		}
	}

	@Test
	void setIterateUpperBound_memorySegment_restrictsIteration(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openReadWrite(dir);
		     var arena = Arena.ofConfined()) {
			db.put("a".getBytes(), "1".getBytes());
			db.put("b".getBytes(), "2".getBytes());
			db.put("c".getBytes(), "3".getBytes());

			try (var readOptions = ReadOptions.newReadOptions()) {
				MemorySegment bound = arena.allocateFrom("b", StandardCharsets.US_ASCII);
				readOptions.setIterateUpperBound(bound.asSlice(0, 1));

				// When
				List<String> keys = new ArrayList<>();
				try (RocksIterator it = db.newIterator(readOptions)) {
					for (it.seekToFirst(); it.isValid(); it.next()) {
						keys.add(new String(it.key()));
					}
					it.checkError();
				}

				// Then
				assertThat(keys).containsExactly("a");
			}
		}
	}

	@Test
	void setIterateLowerBound_byteBuffer_restrictsIteration(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openReadWrite(dir)) {
			db.put("a".getBytes(), "1".getBytes());
			db.put("b".getBytes(), "2".getBytes());
			db.put("c".getBytes(), "3".getBytes());

			try (var readOptions = ReadOptions.newReadOptions()) {
				ByteBuffer bound = ByteBuffer.allocateDirect(1).put((byte) 'b').flip();
				readOptions.setIterateLowerBound(bound);

				// When
				List<String> keys = new ArrayList<>();
				try (RocksIterator it = db.newIterator(readOptions)) {
					for (it.seekToFirst(); it.isValid(); it.next()) {
						keys.add(new String(it.key()));
					}
					it.checkError();
				}

				// Then
				assertThat(keys).containsExactly("b", "c");
			}
		}
	}

	@Test
	void setIterateUpperBound_byteBuffer_restrictsIteration(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openReadWrite(dir)) {
			db.put("a".getBytes(), "1".getBytes());
			db.put("b".getBytes(), "2".getBytes());
			db.put("c".getBytes(), "3".getBytes());

			try (var readOptions = ReadOptions.newReadOptions()) {
				ByteBuffer bound = ByteBuffer.allocateDirect(1).put((byte) 'b').flip();
				readOptions.setIterateUpperBound(bound);

				// When
				List<String> keys = new ArrayList<>();
				try (RocksIterator it = db.newIterator(readOptions)) {
					for (it.seekToFirst(); it.isValid(); it.next()) {
						keys.add(new String(it.key()));
					}
					it.checkError();
				}

				// Then
				assertThat(keys).containsExactly("a");
			}
		}
	}

	@Test
	void setIterateLowerBound_null_clearsBound(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openReadWrite(dir)) {
			db.put("a".getBytes(), "1".getBytes());
			db.put("b".getBytes(), "2".getBytes());

			try (var readOptions = ReadOptions.newReadOptions()) {
				readOptions.setIterateLowerBound("b".getBytes());
				readOptions.setIterateLowerBound((byte[]) null);

				// When
				List<String> keys = new ArrayList<>();
				try (RocksIterator it = db.newIterator(readOptions)) {
					for (it.seekToFirst(); it.isValid(); it.next()) {
						keys.add(new String(it.key()));
					}
					it.checkError();
				}

				// Then
				assertThat(keys).containsExactly("a", "b");
			}
		}
	}

	@Test
	void setIterateUpperBound_null_clearsBound(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.openReadWrite(dir)) {
			db.put("a".getBytes(), "1".getBytes());
			db.put("b".getBytes(), "2".getBytes());

			try (var readOptions = ReadOptions.newReadOptions()) {
				readOptions.setIterateUpperBound("b".getBytes());
				readOptions.setIterateUpperBound((byte[]) null);

				// When
				List<String> keys = new ArrayList<>();
				try (RocksIterator it = db.newIterator(readOptions)) {
					for (it.seekToFirst(); it.isValid(); it.next()) {
						keys.add(new String(it.key()));
					}
					it.checkError();
				}

				// Then
				assertThat(keys).containsExactly("a", "b");
			}
		}
	}
}
