package io.jonasg.kawa.broker;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaRecordReaderTest {

    private static final TopicPartition PARTITION = new TopicPartition("shipments", 1);

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("none");
    private final KafkaRecordReader reader = new KafkaRecordReader(() -> consumer);

    @Test
    void readsWithoutAGroupWithoutCommittingAndWithoutResettingOffsets() {
        // given
        var security = new Properties();
        security.put("security.protocol", "SASL_SSL");
        security.put("group.id", "should-not-be-used");

        // when
        var props = KafkaRecordReader.props("localhost:9092", security);

        // then
        assertThat(props)
                .withFailMessage(() -> "the reader consumer would join a group or commit: " + props)
                .doesNotContainKey("group.id")
                .containsEntry("enable.auto.commit", "false")
                .containsEntry("auto.offset.reset", "none")
                .containsEntry("allow.auto.create.topics", "false")
                .containsEntry("bootstrap.servers", "localhost:9092")
                .containsEntry("security.protocol", "SASL_SSL");
    }

    @Test
    void readsUpToMaxRecordsFromTheStartOffset() {
        // given
        recordsAtOffsets(0, 10);

        // when
        RecordReader.PartitionRead read;
        try (var session = reader.open()) {
            read = session.read("shipments", 1, 2, 9, 3, Duration.ofSeconds(5));
        }

        // then
        assertThat(read.records()).extracting(RecordReader.FetchedRecord::offset)
                .withFailMessage(() -> "not the 3 records from offset 2 were read: " + read).containsExactly(2L, 3L, 4L);
        assertThat(read.nextOffset()).isEqualTo(5);
        assertThat(read.timedOut()).isFalse();
    }

    @Test
    void stopsBeforeTheStopOffset() {
        // given
        recordsAtOffsets(0, 10);

        // when
        RecordReader.PartitionRead read;
        try (var session = reader.open()) {
            read = session.read("shipments", 1, 6, 8, 50, Duration.ofSeconds(5));
        }

        // then
        assertThat(read.records()).extracting(RecordReader.FetchedRecord::offset)
                .withFailMessage(() -> "the read went past the exclusive stop offset: " + read).containsExactly(6L, 7L);
        assertThat(read.nextOffset()).isEqualTo(8);
    }

    @Test
    void reportsAReadThatRunsOutOfTimeAsTimedOut() {
        // given no records

        // when
        RecordReader.PartitionRead read;
        try (var session = reader.open()) {
            read = session.read("shipments", 1, 0, 5, 5, Duration.ofMillis(50));
        }

        // then
        assertThat(read.timedOut()).withFailMessage(() -> "a read that found nothing in time did not time out").isTrue();
        assertThat(read.nextOffset()).isZero();
    }

    @Test
    void reportsTheOffsetsEachPartitionSpans() {
        // given
        consumer.updateBeginningOffsets(Map.of(PARTITION, 4L));
        consumer.updateEndOffsets(Map.of(PARTITION, 20L));

        // when
        Map<Integer, RecordReader.LogRange> ranges;
        try (var session = reader.open()) {
            ranges = session.logRanges("shipments", List.of(1), Duration.ofSeconds(5));
        }

        // then
        assertThat(ranges).containsExactly(Map.entry(1, new RecordReader.LogRange(4, 20)));
    }

    @Test
    void commitsNothingAndClosesTheConsumerWithTheSession() {
        // given
        recordsAtOffsets(0, 3);

        // when
        var session = reader.open();
        session.read("shipments", 1, 0, 3, 3, Duration.ofSeconds(5));
        var committed = consumer.committed(Set.of(PARTITION));
        session.close();

        // then
        assertThat(committed.values()).withFailMessage(() -> "an offset was committed: " + committed)
                .allSatisfy(offset -> assertThat(offset).isNull());
        assertThat(consumer.closed()).withFailMessage(() -> "the consumer was not closed with the session").isTrue();
    }

    /// Records at `from..to-1` of the partition, added once the reader has assigned it.
    private void recordsAtOffsets(long from, long to) {
        consumer.updateBeginningOffsets(Map.of(PARTITION, from));
        consumer.schedulePollTask(() -> {
            for (long offset = from; offset < to; offset++) {
                consumer.addRecord(new ConsumerRecord<>("shipments", 1, offset, new byte[]{1}, new byte[]{2}));
            }
        });
    }
}
