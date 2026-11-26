package io.jonasg.kawa.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/// Consumes the compacted gateway-config topic and delivers each full [DynamicConfig]
/// snapshot to a callback.
///
/// On [start] the consumer reads the topic from the beginning until it has caught up to the
/// end offset that existed at assignment time (the "initial load"). It applies only the latest
/// retained snapshot from that range: older records may still be present because compaction is
/// asynchronous, but they are superseded. [awaitInitialLoad] blocks until that latest snapshot
/// has been applied, so the gateway can refuse to serve until it has applied the config that
/// existed at boot.
///
/// If the latest retained snapshot is invalid or rejected by the callback, initial load fails:
/// falling back to an older snapshot would hide corruption of the authoritative config. After
/// initial load, a bad live message is logged and skipped rather than killing the consumer
/// thread - the gateway keeps its last-known-good config and the next valid message resumes the
/// stream.
///
/// The consumer reads `read_committed`: snapshots are written in transactions (see
/// [TransactionalConfigTopicWriter]), and a write that was fenced by another replica is aborted, so
/// it must never be applied.
///
/// The topic is expected to have a single partition and `cleanup.policy=compact`; ordering
/// within the partition is what makes "apply the last snapshot" correct. The consumer uses
/// direct partition assignment ([KafkaConsumer#assign]) instead of a consumer group, so the
/// gateway never registers a group in the cluster.
public final class ConfigTopicConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConfigTopicConsumer.class);
    private static final String CONFIG_KEY = "config";

    private final org.apache.kafka.clients.consumer.Consumer<String, String> consumer;
    private final String topic;
    private final BiConsumer<DynamicConfig, Long> onConfigWithOffset;
    private final LongConsumer onPosition;
    private final JsonMapper mapper = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();
    private final CountDownLatch initialLoad = new CountDownLatch(1);
    private volatile Exception failure;

    private volatile boolean running;
    private Thread thread;

    /// @param bootstrapServers `host:port` list of the cluster hosting the config topic
    /// @param topic            the config topic name
    /// @param onConfig         invoked for every valid config snapshot, in offset order
    public ConfigTopicConsumer(
            String bootstrapServers,
            String topic,
            Consumer<DynamicConfig> onConfig
    ) {
        this(bootstrapServers, topic, new Properties(), onConfig);
    }

    /// Variant that accepts extra consumer properties (e.g. SASL/security settings for the
    /// config topic) on top of the base bootstrap/deserializer configuration.
    public ConfigTopicConsumer(
            String bootstrapServers,
            String topic,
            Properties extraProps,
            Consumer<DynamicConfig> onConfig
    ) {
        this(bootstrapServers, topic, extraProps, (config, ignoredOffset) -> onConfig.accept(config));
    }

    /// Variant with extra consumer properties that also passes the consumed record offset to
    /// the callback.
    public ConfigTopicConsumer(
            String bootstrapServers,
            String topic,
            Properties extraProps,
            BiConsumer<DynamicConfig, Long> onConfigWithOffset
    ) {
        this(bootstrapServers, topic, extraProps, onConfigWithOffset, ignoredPosition -> { });
    }

    /// Variant that also reports the consumer's position after every poll: the offset of the next
    /// record it will read. The position moves past aborted records and transaction markers too, so
    /// it is what tells whether the consumer has read up to a given offset.
    public ConfigTopicConsumer(
            String bootstrapServers,
            String topic,
            Properties extraProps,
            BiConsumer<DynamicConfig, Long> onConfigWithOffset,
            LongConsumer onPosition
    ) {
        this(topic, new KafkaConsumer<>(consumerProps(bootstrapServers, extraProps)), onConfigWithOffset, onPosition);
    }

    ConfigTopicConsumer(
            String topic,
            org.apache.kafka.clients.consumer.Consumer<String, String> consumer,
            BiConsumer<DynamicConfig, Long> onConfigWithOffset,
            LongConsumer onPosition
    ) {
        this.topic = topic;
        this.onConfigWithOffset = onConfigWithOffset;
        this.onPosition = onPosition;
        this.consumer = consumer;
    }

    /// Starts the consumer thread. Idempotent.
    public void start() {
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(this::run, "kawa-config-consumer");
        thread.setDaemon(true);
        thread.start();
    }

    /// Blocks until the initial catch-up is complete, i.e. every config message that existed
    /// at assignment time has been delivered to the callback.
    ///
    /// @throws IllegalStateException when the consumer failed before catching up, e.g. because it
    ///                               cannot authenticate to the brokers; startup must not hang
    public void awaitInitialLoad() throws InterruptedException {
        initialLoad.await();
        Exception failed = failure;
        if (failed != null) {
            throw new IllegalStateException("Cannot read config topic '" + topic + "': " + failed.getMessage(), failed);
        }
    }

    /// Whether the initial catch-up has completed.
    public boolean initialLoadComplete() {
        return initialLoad.getCount() == 0;
    }

    /// Stops the consumer thread and closes the underlying client. Idempotent; safe to call
    /// before [start] (the client is closed without a thread having run).
    @Override
    public void close() {
        running = false;
        consumer.wakeup();
        if (thread != null) {
            try {
                thread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        consumer.close();
    }

    private void run() {
        try {
            // Direct partition assignment instead of a consumer group: the config topic is a
            // single-partition compacted topic and every boot re-reads from the earliest
            // offset, so no group membership or offset commits are needed. This keeps the
            // gateway from registering a consumer group (and leaving dead groups behind on
            // every restart) in the cluster.
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(Set.of(partition));
            consumer.seekToBeginning(Set.of(partition));
            long targetEnd = consumer.endOffsets(Set.of(partition)).get(partition);
            boolean caughtUp = false;
            ConsumerRecord<String, String> latestInitialRecord = null;
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                var liveRecords = new ArrayList<ConsumerRecord<String, String>>();
                for (ConsumerRecord<String, String> record : records) {
                    if (!isConfigRecord(record)) {
                        log.warn("ignoring record with unexpected key '{}' at offset {} on config topic {}",
                                record.key(), record.offset(), topic);
                    } else if (!caughtUp && record.offset() < targetEnd) {
                        latestInitialRecord = record;
                    } else if (!caughtUp) {
                        liveRecords.add(record);
                    } else {
                        handle(record);
                    }
                }
                long position = consumer.position(partition);
                if (!caughtUp && position >= targetEnd) {
                    applyInitialRecord(latestInitialRecord);
                    caughtUp = true;
                    liveRecords.forEach(this::handle);
                    onPosition.accept(position);
                    initialLoad.countDown();
                    log.info("initial config load complete for topic {} at offset {}", topic, targetEnd);
                } else {
                    onPosition.accept(position);
                }
            }
        } catch (WakeupException e) {
            // expected on close()
        } catch (Exception e) {
            log.error("config consumer for topic {} failed", topic, e);
            if (initialLoad.getCount() > 0) {
                failure = e;
                initialLoad.countDown();
            }
        } finally {
            consumer.close();
        }
    }

    /// Deserializes one message and delivers it to the callback. Package-private so the
    /// message-handling path is testable without a broker.
    void handle(ConsumerRecord<String, String> record) {
        try {
            apply(record);
        } catch (Exception e) {
            log.error("skipping invalid config message at offset {} on topic {}", record.offset(), topic, e);
        }
    }

    private void applyInitialRecord(ConsumerRecord<String, String> record) throws Exception {
        if (record != null) {
            apply(record);
        }
    }

    private void apply(ConsumerRecord<String, String> record) throws Exception {
        DynamicConfig config = mapper.readValue(record.value(), DynamicConfig.class);
        onConfigWithOffset.accept(config, record.offset());
    }

    private static boolean isConfigRecord(ConsumerRecord<String, String> record) {
        return CONFIG_KEY.equals(record.key());
    }

    private static Properties consumerProps(String bootstrapServers, Properties extraProps) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.putAll(extraProps);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
        return props;
    }
}
