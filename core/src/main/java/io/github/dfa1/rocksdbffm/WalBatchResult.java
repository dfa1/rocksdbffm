package io.github.dfa1.rocksdbffm;

/// Holds the result of [WalIterator#getBatch()]: a [WriteBatch] and the sequence number
/// of the first transaction in that batch.
///
/// The caller owns the [WriteBatch] and must close it when done.
///
/// ```
/// try (WalIterator it = db.getUpdatesSince(seq)) {
///     for (; it.isValid(); it.next()) {
///         try (WalBatchResult result = it.getBatch()) {
///             SequenceNumber seq = result.sequenceNumber();
///             WriteBatch batch   = result.writeBatch();
///         }
///     }
///     it.checkStatus();
/// }
/// ```
public record WalBatchResult(SequenceNumber sequenceNumber, WriteBatch writeBatch)
		implements AutoCloseable {

	@Override
	public void close() {
		writeBatch.close();
	}
}
