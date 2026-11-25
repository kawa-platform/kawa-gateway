package io.jonasg.kawa.test;

import io.jonasg.kawa.broker.RecordReader;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/// In-memory [RecordReader] for tool tests: each partition is a log of records appended through
/// [#append] or [#appendAt] (which leaves offset gaps, as compaction does). Sessions are counted, so a
/// test can check that every one was closed, and reads can be made to fail or time out per partition.
public final class FakeRecordReader implements RecordReader {

    private final Map<String, Map<Integer, Log>> topics = new HashMap<>();
    /// Partitions whose reads time out without returning a record.
    public final Set<Integer> timingOut = new HashSet<>();
    /// Partitions whose reads fail, as a broker refusing the fetch would.
    public final Set<Integer> failing = new HashSet<>();
    /// When set, looking up log ranges fails with it, as a broker that cannot be reached would.
    public @Nullable RuntimeException rangeError;
    /// Runs before the first read of every session, e.g. to append records while an inspection runs.
    public @Nullable Runnable beforeFirstRead;
    public int opened;
    public int closed;
    /// The reads every session made, as `partition@fromOffset`.
    public final List<String> reads = new ArrayList<>();

    /// Appends a record with UTF-8 `key` and `value` (either may be `null`) at the partition's end offset.
    public FakeRecordReader append(String topic, int partition, Instant timestamp, @Nullable String key, @Nullable String value) {
        return append(topic, partition, timestamp, utf8(key), utf8(value));
    }

    /// Appends a record with raw `key` and `value` bytes at the partition's end offset.
    public FakeRecordReader append(String topic, int partition, Instant timestamp, byte @Nullable [] key, byte @Nullable [] value) {
        var log = log(topic, partition);
        return appendAt(topic, partition, log.end, timestamp, key, value);
    }

    /// Appends a record at `offset`, which must be at or beyond the end offset; the offsets skipped become a gap.
    public FakeRecordReader appendAt(String topic, int partition, long offset, Instant timestamp, byte @Nullable [] key,
                                     byte @Nullable [] value) {
        var log = log(topic, partition);
        if (offset < log.end) {
            throw new IllegalArgumentException("offset " + offset + " is below the end offset " + log.end);
        }
        log.records.put(offset, new FetchedRecord(partition, offset, timestamp.toEpochMilli(), "CreateTime", key, value));
        log.end = offset + 1;
        return this;
    }

    /// Creates the partitions `0..partitions-1` of `topic`, empty.
    public FakeRecordReader topic(String topic, int partitions) {
        for (int partition = 0; partition < partitions; partition++) {
            log(topic, partition);
        }
        return this;
    }

    /// Deletes the records below `offset`, as retention does.
    public void deleteBefore(String topic, int partition, long offset) {
        var log = log(topic, partition);
        log.records.headMap(offset).clear();
        log.start = Math.max(log.start, offset);
    }

    private Log log(String topic, int partition) {
        return topics.computeIfAbsent(topic, _ -> new HashMap<>()).computeIfAbsent(partition, _ -> new Log());
    }

    private static byte @Nullable [] utf8(@Nullable String text) {
        return text == null ? null : text.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Session open() {
        opened++;
        return new FakeSession();
    }

    private static final class Log {
        private final TreeMap<Long, FetchedRecord> records = new TreeMap<>();
        private long start;
        private long end;
    }

    private final class FakeSession implements Session {

        private boolean read;

        @Override
        public Map<Integer, LogRange> logRanges(String topic, Collection<Integer> partitions, Duration timeout) {
            if (rangeError != null) {
                throw rangeError;
            }
            Map<Integer, LogRange> ranges = new HashMap<>();
            partitions.forEach(partition -> {
                var log = log(topic, partition);
                ranges.put(partition, new LogRange(log.start, log.end));
            });
            return ranges;
        }

        @Override
        public Map<Integer, Long> offsetsForTime(String topic, Collection<Integer> partitions, Instant time, Duration timeout) {
            Map<Integer, Long> offsets = new HashMap<>();
            partitions.forEach(partition -> log(topic, partition).records.values().stream()
                    .filter(record -> record.timestamp() >= time.toEpochMilli())
                    .findFirst()
                    .ifPresent(record -> offsets.put(partition, record.offset())));
            return offsets;
        }

        @Override
        public PartitionRead read(String topic, int partition, long fromOffset, long stopOffset, int maxRecords, Duration timeout) {
            if (!read && beforeFirstRead != null) {
                beforeFirstRead.run();
            }
            read = true;
            reads.add(partition + "@" + fromOffset);
            if (failing.contains(partition)) {
                throw new IllegalStateException("partition " + partition + " cannot be read");
            }
            if (timingOut.contains(partition)) {
                return new PartitionRead(List.of(), fromOffset, true);
            }
            var log = log(topic, partition);
            if (fromOffset < log.start) {
                throw new IllegalStateException("offset " + fromOffset + " is out of range");
            }
            List<FetchedRecord> records = new ArrayList<>();
            long next = Math.max(fromOffset, Math.min(stopOffset, log.end));
            for (FetchedRecord record : log.records.subMap(fromOffset, stopOffset).values()) {
                if (records.size() == maxRecords) {
                    next = records.getLast().offset() + 1;
                    break;
                }
                records.add(record);
            }
            if (records.size() == maxRecords && !records.isEmpty()) {
                next = records.getLast().offset() + 1;
            }
            return new PartitionRead(records, fromOffset >= stopOffset ? fromOffset : next, false);
        }

        @Override
        public void close() {
            closed++;
        }
    }
}
