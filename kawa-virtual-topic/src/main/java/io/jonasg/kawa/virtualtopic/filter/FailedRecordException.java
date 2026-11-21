package io.jonasg.kawa.virtualtopic.filter;

import org.apache.kafka.common.record.internal.MemoryRecords;

/// A record of a fetched partition could not be filtered or transformed under the `fail`
/// [io.jonasg.kawa.config.DecodeErrorPolicy]: the partition stops at that record.
///
/// Carries the records of the partition before the bad one, rewritten as usual, which end right
/// before [#offset()] so the consumer's next fetch starts at the bad record. They are empty when
/// the bad record is the first record of the partition's fetched data.
public final class FailedRecordException extends RuntimeException {

    private final long offset;
    private final transient MemoryRecords recordsBefore;

    public FailedRecordException(long offset, MemoryRecords recordsBefore, RuntimeException cause) {
        super("record at offset " + offset + " cannot be rewritten: " + cause.getMessage(), cause);
        this.offset = offset;
        this.recordsBefore = recordsBefore;
    }

    /// The offset of the bad record.
    public long offset() {
        return offset;
    }

    /// The records before the bad one, ending right before it.
    public MemoryRecords recordsBefore() {
        return recordsBefore;
    }
}
