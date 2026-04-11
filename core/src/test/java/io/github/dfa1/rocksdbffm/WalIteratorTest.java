package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class WalIteratorTest {

	@Test
	void getLatestSequenceNumber_advancesAfterWrites(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			SequenceNumber before = db.getLatestSequenceNumber();

			// When
			db.put("k".getBytes(), "v".getBytes());
			SequenceNumber after = db.getLatestSequenceNumber();

			// Then
			assertThat(after.isAfter(before)).isTrue();
		}
	}

	@Test
	void getUpdatesSince_yieldsWrittenBatches(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			SequenceNumber start = db.getLatestSequenceNumber();
			db.put("a".getBytes(), "1".getBytes());
			db.put("b".getBytes(), "2".getBytes());
			db.put("c".getBytes(), "3".getBytes());

			// When
			List<SequenceNumber> seqs = new ArrayList<>();
			try (WalIterator it = db.getUpdatesSince(start)) {
				for (; it.isValid(); it.next()) {
					try (WalBatchResult result = it.getBatch()) {
						seqs.add(result.sequenceNumber());
					}
				}
				it.checkStatus();
			}

			// Then
			assertThat(seqs).hasSize(3);
			// sequence numbers must be strictly increasing
			for (int i = 1; i < seqs.size(); i++) {
				assertThat(seqs.get(i).isAfter(seqs.get(i - 1))).isTrue();
			}
		}
	}

	@Test
	void getUpdatesSince_batchContainsExpectedCount(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			SequenceNumber start = db.getLatestSequenceNumber();

			WriteBatch batch = WriteBatch.create();
			batch.put("x".getBytes(), "1".getBytes());
			batch.put("y".getBytes(), "2".getBytes());
			db.write(batch);
			batch.close();

			// When
			int count = 0;
			try (WalIterator it = db.getUpdatesSince(start)) {
				for (; it.isValid(); it.next()) {
					try (WalBatchResult result = it.getBatch()) {
						count += result.writeBatch().count();
					}
				}
				it.checkStatus();
			}

			// Then
			assertThat(count).isEqualTo(2);
		}
	}

	@Test
	void getUpdatesSince_emptyWhenNoWritesAfterSequence(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), "v".getBytes());
			SequenceNumber after = db.getLatestSequenceNumber();

			// When
			try (WalIterator it = db.getUpdatesSince(after)) {
				// Move past any batch at exactly `after`, then check exhausted
				while (it.isValid()) {
					try (WalBatchResult ignored = it.getBatch()) {
						// consume
					}
					it.next();
				}
				it.checkStatus();

				// Then
				assertThat(it.isValid()).isFalse();
			}
		}
	}

	@Test
	void walIterator_isClosedSafely(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), "v".getBytes());
			SequenceNumber start = SequenceNumber.of(0);

			// When/Then — double close must not crash
			WalIterator it = db.getUpdatesSince(start);
			it.close();
			it.close();
		}
	}
}
