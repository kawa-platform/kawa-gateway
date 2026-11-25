package io.jonasg.kawa.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

/// The Kafka [ConfigTopicWriter]: writes full [DynamicConfig] snapshots to the compacted
/// gateway-config topic, each in its own transaction. The consumer side ([ConfigTopicConsumer])
/// reads `read_committed` and applies each snapshot through the normal
/// `io.jonasg.kawa.server.DynamicConfigManager` flow, so a commit here is a live config change.
///
/// Every snapshot is sent with the same key so compaction keeps only the latest one. All replicas
/// writing to one topic share the `transactional.id` `kawa-config-writer-<topic>`: initializing the
/// producer (taking over) fences every other replica's producer, so whoever took over last is the
/// only one that can commit. A write that loses to another writer fails with
/// [ConfigWriteFencedException], a conflict the caller resolves by retrying on the newer snapshot.
/// It is reported as such when the broker fences the producer (`ProducerFencedException`,
/// `InvalidProducerEpochException`) or reports the transaction in an invalid state
/// (`InvalidTxnStateException`), which concurrent takeovers can produce instead of a fence. The
/// fenced producer is closed and the next call takes over again with a fresh one.
///
/// Requires brokers with transaction support and, where ACLs are enforced, `WRITE` and `DESCRIBE` on
/// that transactional id for the gateway's principal.
public final class TransactionalConfigTopicWriter implements ConfigTopicWriter {

    private static final Logger log = LoggerFactory.getLogger(TransactionalConfigTopicWriter.class);

    private static final String CONFIG_KEY = "config";
    private static final Duration OFFSETS_TIMEOUT = Duration.ofSeconds(10);

    private final String topic;
    private final TopicPartition partition;
    private final Supplier<Producer<String, String>> producers;
    private final Consumer<String, String> offsets;
    private final JsonMapper mapper = JsonMapper.builder().build();

    private @Nullable Producer<String, String> producer;

    /// @param bootstrapServers `host:port` list of the cluster hosting the config topic
    /// @param topic            the config topic name
    /// @param extraProps       extra client properties (e.g. SASL/security settings for the config
    ///                         topic) on top of the base configuration
    public TransactionalConfigTopicWriter(String bootstrapServers, String topic, Properties extraProps) {
        this(topic, producerFactory(producerProps(bootstrapServers, topic, extraProps)),
                new KafkaConsumer<>(offsetsConsumerProps(bootstrapServers, extraProps)));
    }

    /// @param topic     the config topic name
    /// @param producers supplies a new, uninitialized transactional producer whenever this writer takes
    ///                  over: initially and after giving up a fenced producer. The writer calls
    ///                  `initTransactions()` on it.
    /// @param offsets   the `read_committed` consumer [#committedEndOffset()] reads the end offset with
    TransactionalConfigTopicWriter(String topic, Supplier<Producer<String, String>> producers, Consumer<String, String> offsets) {
        this.topic = topic;
        this.partition = new TopicPartition(topic, 0);
        this.producers = producers;
        this.offsets = offsets;
    }

    private static Supplier<Producer<String, String>> producerFactory(Properties props) {
        return () -> new KafkaProducer<>(props);
    }

    /// The `transactional.id` every replica writing to `topic` shares.
    static String transactionalId(String topic) {
        return "kawa-config-writer-" + topic;
    }

    @Override
    public synchronized long commit(DynamicConfig config) {
        Producer<String, String> current = takeOver();
        try {
            current.beginTransaction();
            RecordMetadata metadata = current.send(new ProducerRecord<>(topic, CONFIG_KEY, serialize(config))).get();
            current.commitTransaction();
            return metadata.offset();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            abort(current);
            throw new IllegalStateException("interrupted while writing config snapshot to topic " + topic, e);
        } catch (Exception e) {
            if (isFenced(e)) {
                closeProducer();
                throw new ConfigWriteFencedException("another writer took over the config topic " + topic, e);
            }
            abort(current);
            throw new IllegalStateException("failed to write config snapshot to topic " + topic, e);
        }
    }

    @Override
    public synchronized long committedEndOffset() {
        takeOver();
        // Under read_committed the end offset is the last stable offset: past every committed record,
        // before any transaction still open.
        return offsets.endOffsets(Set.of(partition), OFFSETS_TIMEOUT).get(partition);
    }

    /// The producer, initialized now if this writer has none: initializing fences every other
    /// writer sharing the transactional id.
    private Producer<String, String> takeOver() {
        if (producer == null) {
            Producer<String, String> created = producers.get();
            try {
                created.initTransactions();
            } catch (RuntimeException e) {
                created.close(Duration.ZERO);
                throw new IllegalStateException("failed to initialize the config writer for topic " + topic, e);
            }
            producer = created;
        }
        return producer;
    }

    private void abort(Producer<String, String> current) {
        try {
            current.abortTransaction();
        } catch (RuntimeException e) {
            log.warn("failed to abort config write on topic {}; recreating the writer", topic, e);
            closeProducer();
        }
    }

    private void closeProducer() {
        if (producer != null) {
            producer.close(Duration.ZERO);
            producer = null;
        }
    }

    /// Whether the write lost to another writer sharing the transactional id, anywhere in the cause chain.
    ///
    /// Besides the fences (`ProducerFencedException`, `InvalidProducerEpochException`), concurrent
    /// takeovers can make the broker report this writer's transaction as `InvalidTxnStateException`,
    /// from the send as well as from the commit. Treating that as a conflict is safe: the transaction
    /// is no longer ongoing, so the snapshot was not committed and the caller's retry cannot apply it
    /// twice. Only another writer sharing the transactional id can put the transaction in that state,
    /// as this writer is the only one using its producer and does so under the writer's lock.
    private static boolean isFenced(Throwable e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof ProducerFencedException
                    || cause instanceof InvalidProducerEpochException
                    || cause instanceof InvalidTxnStateException) {
                return true;
            }
        }
        return false;
    }

    /// Serializes a snapshot to the JSON form the consumer deserializes. Package-private so the
    /// message-encoding path is testable without a broker.
    String serialize(DynamicConfig config) {
        try {
            return mapper.writeValueAsString(config);
        } catch (Exception e) {
            throw new IllegalStateException("failed to serialize config snapshot", e);
        }
    }

    @Override
    public synchronized void close() {
        closeProducer();
        offsets.close();
    }

    private static Properties producerProps(String bootstrapServers, String topic, Properties extraProps) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.putAll(extraProps);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId(topic));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return props;
    }

    private static Properties offsetsConsumerProps(String bootstrapServers, Properties extraProps) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.putAll(extraProps);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
        return props;
    }
}
