package io.jonasg.kawa.broker;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

/// [RecordReader] backed by a Kafka consumer per [Session], over the same bootstrap servers and broker
/// credentials (`security.protocol`, `sasl.*`, `ssl.*`) as the gateway.
///
/// The consumer has no `group.id`: it assigns partitions itself and never joins a group, so it cannot
/// commit offsets (Kafka refuses a commit without a group) and no consumer group sees it. Auto commit is
/// off and `auto.offset.reset` is `none`, so a read of an offset retention has deleted fails instead of
/// silently jumping to another offset.
public final class KafkaRecordReader implements RecordReader {

    /// The client id the broker logs and quotas see for inspection reads.
    static final String CLIENT_ID = "kawa-record-reader";

    private static final Duration MAX_POLL = Duration.ofMillis(500);

    private final Supplier<Consumer<byte[], byte[]>> consumers;

    /// @param brokerSecurity the Kafka client settings for reaching the brokers, see `BrokerSecurity.clientProperties`
    public KafkaRecordReader(String bootstrapServers, Properties brokerSecurity) {
        this(() -> new KafkaConsumer<>(props(bootstrapServers, brokerSecurity)));
    }

    /// Package-private for tests.
    KafkaRecordReader(Supplier<Consumer<byte[], byte[]>> consumers) {
        this.consumers = consumers;
    }

    @Override
    public Session open() {
        return new KafkaSession(consumers.get());
    }

    /// Consumer properties: the broker connection settings plus a group-less, non-committing, byte-level reader.
    static Properties props(String bootstrapServers, Properties brokerSecurity) {
        Properties props = new Properties();
        props.putAll(brokerSecurity);
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.remove(ConsumerConfig.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return props;
    }

    private static final class KafkaSession implements Session {

        private final Consumer<byte[], byte[]> consumer;

        KafkaSession(Consumer<byte[], byte[]> consumer) {
            this.consumer = consumer;
        }

        @Override
        public Map<Integer, LogRange> logRanges(String topic, Collection<Integer> partitions, Duration timeout) {
            var topicPartitions = partitions.stream().map(partition -> new TopicPartition(topic, partition)).toList();
            var starts = consumer.beginningOffsets(topicPartitions, timeout);
            var ends = consumer.endOffsets(topicPartitions, timeout);
            Map<Integer, LogRange> ranges = new HashMap<>();
            for (TopicPartition partition : topicPartitions) {
                Long start = starts.get(partition);
                Long end = ends.get(partition);
                if (start != null && end != null) {
                    ranges.put(partition.partition(), new LogRange(start, end));
                }
            }
            return ranges;
        }

        @Override
        public Map<Integer, Long> offsetsForTime(String topic, Collection<Integer> partitions, Instant time, Duration timeout) {
            Map<TopicPartition, Long> query = new HashMap<>();
            partitions.forEach(partition -> query.put(new TopicPartition(topic, partition), time.toEpochMilli()));
            Map<Integer, Long> offsets = new HashMap<>();
            consumer.offsetsForTimes(query, timeout).forEach((partition, found) -> {
                if (found != null) {
                    offsets.put(partition.partition(), found.offset());
                }
            });
            return offsets;
        }

        @Override
        public PartitionRead read(String topic, int partition, long fromOffset, long stopOffset, int maxRecords, Duration timeout) {
            List<FetchedRecord> records = new ArrayList<>();
            if (fromOffset >= stopOffset || maxRecords <= 0) {
                return new PartitionRead(records, fromOffset, false);
            }
            var topicPartition = new TopicPartition(topic, partition);
            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, fromOffset);
            long deadline = System.nanoTime() + timeout.toNanos();
            long next = fromOffset;
            while (next < stopOffset && records.size() < maxRecords) {
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    return new PartitionRead(records, next, true);
                }
                var polled = consumer.poll(Duration.ofNanos(Math.min(left, MAX_POLL.toNanos())));
                for (ConsumerRecord<byte[], byte[]> record : polled.records(topicPartition)) {
                    if (record.offset() >= stopOffset || records.size() >= maxRecords) {
                        break;
                    }
                    records.add(fetched(record));
                    next = record.offset() + 1;
                }
                if (records.size() < maxRecords) {
                    // Every polled record below stopOffset was taken, so the position also covers the gaps
                    // (compacted offsets, transaction markers) the poll skipped.
                    next = Math.max(next, Math.min(consumer.position(topicPartition, timeout), stopOffset));
                }
            }
            return new PartitionRead(records, next, false);
        }

        private static FetchedRecord fetched(ConsumerRecord<byte[], byte[]> record) {
            return new FetchedRecord(
                    record.partition(),
                    record.offset(),
                    record.timestamp(),
                    record.timestampType().name,
                    record.key(),
                    record.value());
        }

        @Override
        public void close() {
            consumer.close();
        }
    }
}
