package io.jonasg.kawa.broker;

import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/// Reads records of a physical topic for inspection, never as a member of a consumer group: a reader
/// commits no offsets and cannot disturb the consumers of the topic. [KafkaRecordReader] reads with a
/// group-less Kafka consumer over the gateway's broker connection settings.
///
/// The work of one inspection runs in one [Session], which holds the broker connection until it is closed.
public interface RecordReader {

    /// Opens a session; the caller closes it.
    Session open();

    /// The broker connection of one inspection. Not thread-safe.
    interface Session extends AutoCloseable {

        /// The offsets each partition of `topic` currently spans, by partition.
        ///
        /// @throws RuntimeException when the broker does not answer within `timeout`
        Map<Integer, LogRange> logRanges(String topic, Collection<Integer> partitions, Duration timeout);

        /// Per partition, the first offset whose record timestamp is at or after `time`. A partition
        /// without such a record is left out.
        ///
        /// @throws RuntimeException when the broker does not answer within `timeout`
        Map<Integer, Long> offsetsForTime(String topic, Collection<Integer> partitions, Instant time, Duration timeout);

        /// Reads up to `maxRecords` records of one partition from `fromOffset`, stopping before `stopOffset`.
        /// Offsets may have gaps (compaction, transaction markers), so fewer records than `stopOffset - fromOffset`
        /// may exist.
        ///
        /// @throws RuntimeException when the broker refuses the read, e.g. because `fromOffset` was deleted
        PartitionRead read(String topic, int partition, long fromOffset, long stopOffset, int maxRecords, Duration timeout);

        @Override
        void close();
    }

    /// The offsets a partition spans.
    ///
    /// @param start the earliest offset still in the log
    /// @param end   the offset the next record written will get: the log holds offsets in `[start, end)`
    record LogRange(long start, long end) {
    }

    /// What one [Session#read] returned.
    ///
    /// @param records    the records read, in offset order
    /// @param nextOffset the offset the partition was read up to (exclusive): every offset below it from the
    ///                   read's start was covered, records or gaps
    /// @param timedOut   whether the read stopped at its timeout before `maxRecords` or `stopOffset` was reached
    record PartitionRead(List<FetchedRecord> records, long nextOffset, boolean timedOut) {
    }

    /// A record as the broker stores it.
    ///
    /// @param partition     the partition it was read from
    /// @param offset        its offset
    /// @param timestamp     its Kafka record timestamp in epoch milliseconds, `-1` when it has none
    /// @param timestampType what the timestamp means: `CreateTime`, `LogAppendTime` or `NoTimestampType`
    /// @param key           the key bytes, `null` for a null key
    /// @param value         the value bytes, `null` for a null value (a tombstone)
    record FetchedRecord(
            int partition,
            long offset,
            long timestamp,
            String timestampType,
            byte @Nullable [] key,
            byte @Nullable [] value
    ) {
    }
}
