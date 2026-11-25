package io.jonasg.kawa.config;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class TransactionalConfigTopicWriterTest {

    private static final String TOPIC = "__kawa";

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void serializeRoundTripsThroughConsumerDeserialization() throws Exception {
        // given
        try (var writer = new TransactionalConfigTopicWriter("localhost:9092", "__kawa", new Properties())) {
            var config = new DynamicConfig(
                    Map.of("orders", new VirtualTopicConfig("raw-orders")),
                    new AuthConfig(Set.of("PLAIN"),
                            Map.of("alice", new ClientConfig("PLAIN",
                                    HashedPassword.fromPlaintext(Mechanism.PLAIN, "secret")))),
                    new RbacConfig(Map.of("reader", new RoleConfig(List.of())), null), null);

            // when
            String json = writer.serialize(config);
            DynamicConfig readBack = mapper.readValue(json, DynamicConfig.class);

            // then
            assertThat(readBack).isEqualTo(config);
        }
    }

    @Test
    void serializeRoundTripsVirtualTopicValueFormat() throws Exception {
        // given a virtual topic filtering on JSON content
        try (var writer = new TransactionalConfigTopicWriter("localhost:9092", "__kawa", new Properties())) {
            var virtualTopic = new VirtualTopicConfig(
                    "orders",
                    new CelFilterConfig("value.status == \"PAID\""),
                    false,
                    new JsonFormatConfig(DecodeErrorPolicy.INCLUDE));
            var config = DynamicConfig.empty().upsertVirtualTopic("paid-orders", virtualTopic);

            // when
            String json = writer.serialize(config);
            DynamicConfig readBack = mapper.readValue(json, DynamicConfig.class);

            // then
            assertThat(readBack.virtualTopics().get("paid-orders"))
                    .withFailMessage(() -> "Virtual topic valueFormat did not survive the config-topic round trip: " + json)
                    .isEqualTo(virtualTopic);
        }
    }

    @Test
    void defaultsOnDecodeErrorToSkipWhenOmitted() throws Exception {
        // given a snapshot written without onDecodeError
        String json = """
                {"virtualTopics": {"paid-orders": {
                  "topic": "orders",
                  "filter": {"type": "cel", "expression": "value.status == \\"PAID\\""},
                  "valueFormat": {"type": "json"}
                }}}
                """;

        // when
        DynamicConfig config = mapper.readValue(json, DynamicConfig.class);

        // then
        assertThat(config.virtualTopics().get("paid-orders").valueFormat())
                .withFailMessage(() -> "Omitted onDecodeError did not default to skip")
                .isEqualTo(new JsonFormatConfig(DecodeErrorPolicy.SKIP));
    }

    @Test
    void serializeProducesFullSnapshotWithAllDynamicSections() throws Exception {
        // given
        try (var writer = new TransactionalConfigTopicWriter("localhost:9092", "__kawa", new Properties())) {
            var config = new DynamicConfig(
                    Map.of("orders", new VirtualTopicConfig("raw-orders")),
                    new AuthConfig(Set.of("PLAIN"),
                            Map.of("alice", new ClientConfig("PLAIN",
                                    HashedPassword.fromPlaintext(Mechanism.PLAIN, "secret")))),
                    new RbacConfig(Map.of("reader", new RoleConfig(List.of())), null), null);

            // when
            String json = writer.serialize(config);

            // then
            assertThat(json).contains("\"virtualTopics\"", "\"auth\"", "\"rbac\"");
        }
    }

    @Test
    void reportsConflictWhenSendFailsWithInvalidTxnState() {
        // given
        var producer = autoCompletingProducer();
        producer.sendException = new InvalidTxnStateException("transaction is in an invalid state");
        try (var writer = writer(producer)) {

            // when
            Throwable thrown = catchThrowable(() -> writer.commit(DynamicConfig.empty()));

            // then
            assertThat(thrown)
                    .withFailMessage(() -> "Send failing with InvalidTxnStateException was not reported as a conflict, got: " + thrown)
                    .isInstanceOf(ConfigWriteFencedException.class);
        }
    }

    @Test
    void reportsConflictWhenSendCompletesWithInvalidTxnState() {
        // given the broker rejects the record asynchronously, through the send future and callback
        var producer = new FailingSendProducer(new InvalidTxnStateException("transaction is in an invalid state"));
        try (var writer = writer(producer)) {

            // when
            Throwable thrown = catchThrowable(() -> writer.commit(DynamicConfig.empty()));

            // then
            assertThat(thrown)
                    .withFailMessage(() -> "Send completing with InvalidTxnStateException was not reported as a conflict, got: " + thrown)
                    .isInstanceOf(ConfigWriteFencedException.class);
        }
    }

    @Test
    void reportsConflictWhenCommitFailsWithInvalidTxnState() {
        // given
        var producer = autoCompletingProducer();
        producer.commitTransactionException = new InvalidTxnStateException("transaction is in an invalid state");
        try (var writer = writer(producer)) {

            // when
            Throwable thrown = catchThrowable(() -> writer.commit(DynamicConfig.empty()));

            // then
            assertThat(thrown)
                    .withFailMessage(() -> "Commit failing with InvalidTxnStateException was not reported as a conflict, got: " + thrown)
                    .isInstanceOf(ConfigWriteFencedException.class);
        }
    }

    @Test
    void reportsConflictWhenCommitFailsWithProducerFenced() {
        // given
        var producer = autoCompletingProducer();
        producer.commitTransactionException = new ProducerFencedException("another replica took over");
        try (var writer = writer(producer)) {

            // when
            Throwable thrown = catchThrowable(() -> writer.commit(DynamicConfig.empty()));

            // then
            assertThat(thrown)
                    .withFailMessage(() -> "Commit failing with ProducerFencedException was not reported as a conflict, got: " + thrown)
                    .isInstanceOf(ConfigWriteFencedException.class);
        }
    }

    @Test
    void reportsConflictWhenSendFailsWithProducerFenced() {
        // given
        var producer = autoCompletingProducer();
        producer.sendException = new ProducerFencedException("another replica took over");
        try (var writer = writer(producer)) {

            // when
            Throwable thrown = catchThrowable(() -> writer.commit(DynamicConfig.empty()));

            // then
            assertThat(thrown)
                    .withFailMessage(() -> "Send failing with ProducerFencedException was not reported as a conflict, got: " + thrown)
                    .isInstanceOf(ConfigWriteFencedException.class);
        }
    }

    @Test
    void reportsConflictWhenCommitFailsWithInvalidProducerEpoch() {
        // given
        var producer = autoCompletingProducer();
        producer.commitTransactionException = new InvalidProducerEpochException("producer epoch is stale");
        try (var writer = writer(producer)) {

            // when
            Throwable thrown = catchThrowable(() -> writer.commit(DynamicConfig.empty()));

            // then
            assertThat(thrown)
                    .withFailMessage(() -> "Commit failing with InvalidProducerEpochException was not reported as a conflict, got: " + thrown)
                    .isInstanceOf(ConfigWriteFencedException.class);
        }
    }

    @Test
    void reportsConflictWhenSendFailsWithInvalidProducerEpoch() {
        // given
        var producer = autoCompletingProducer();
        producer.sendException = new InvalidProducerEpochException("producer epoch is stale");
        try (var writer = writer(producer)) {

            // when
            Throwable thrown = catchThrowable(() -> writer.commit(DynamicConfig.empty()));

            // then
            assertThat(thrown)
                    .withFailMessage(() -> "Send failing with InvalidProducerEpochException was not reported as a conflict, got: " + thrown)
                    .isInstanceOf(ConfigWriteFencedException.class);
        }
    }

    @Test
    void doesNotReportConflictWhenSendFailsWithOtherKafkaError() {
        // given
        var producer = autoCompletingProducer();
        producer.sendException = new KafkaException("broker unavailable");
        try (var writer = writer(producer)) {

            // when
            Throwable thrown = catchThrowable(() -> writer.commit(DynamicConfig.empty()));

            // then
            assertThat(thrown)
                    .withFailMessage(() -> "Send failing with a plain KafkaException did not fail the write")
                    .isNotNull();
            assertThat(thrown)
                    .withFailMessage(() -> "Send failing with a plain KafkaException was reported as a conflict")
                    .isNotInstanceOf(ConfigWriteFencedException.class);
        }
    }

    @Test
    void writesWithFreshProducerAfterInvalidTxnStateConflict() {
        // given a writer whose first producer is rejected with an invalid transaction state on commit
        var stale = autoCompletingProducer();
        stale.commitTransactionException = new InvalidTxnStateException("transaction is in an invalid state");
        var fresh = autoCompletingProducer();
        var producers = new RecordingProducers(stale, fresh);
        try (var writer = new TransactionalConfigTopicWriter(TOPIC, producers, offsets())) {
            Throwable conflict = catchThrowable(() -> writer.commit(DynamicConfig.empty()));
            assertThat(conflict)
                    .withFailMessage(() -> "First write was not reported as a conflict, got: " + conflict)
                    .isInstanceOf(ConfigWriteFencedException.class);

            // when
            long offset = writer.commit(DynamicConfig.empty());

            // then
            assertThat(producers.handedOut())
                    .withFailMessage(() -> "Writer did not take a fresh producer after the conflict; producers handed out: "
                            + producers.handedOut().size())
                    .containsExactly(stale, fresh);
            assertThat(fresh.transactionInitialized())
                    .withFailMessage(() -> "Writer did not initialize transactions on the fresh producer")
                    .isTrue();
            assertThat(fresh.transactionCommitted())
                    .withFailMessage(() -> "Writer did not commit the retried snapshot with the fresh producer")
                    .isTrue();
            assertThat(fresh.history())
                    .withFailMessage(() -> "Writer did not send exactly one snapshot record with the fresh producer")
                    .hasSize(1);
            assertThat(offset)
                    .withFailMessage(() -> "Retried write did not return the offset of the record it wrote, got: " + offset)
                    .isZero();
        }
    }

    @Test
    void writesWithFreshProducerAfterSendConflict() {
        // given a writer whose first producer is rejected with an invalid transaction state on send
        var stale = autoCompletingProducer();
        stale.sendException = new InvalidTxnStateException("transaction is in an invalid state");
        var fresh = autoCompletingProducer();
        var producers = new RecordingProducers(stale, fresh);
        try (var writer = new TransactionalConfigTopicWriter(TOPIC, producers, offsets())) {
            Throwable conflict = catchThrowable(() -> writer.commit(DynamicConfig.empty()));
            assertThat(conflict)
                    .withFailMessage(() -> "First write was not reported as a conflict, got: " + conflict)
                    .isInstanceOf(ConfigWriteFencedException.class);

            // when
            long offset = writer.commit(DynamicConfig.empty());

            // then
            assertThat(producers.handedOut())
                    .withFailMessage(() -> "Writer did not take a fresh producer after the conflict; producers handed out: "
                            + producers.handedOut().size())
                    .containsExactly(stale, fresh);
            assertThat(fresh.transactionCommitted())
                    .withFailMessage(() -> "Writer did not commit the retried snapshot with the fresh producer")
                    .isTrue();
            assertThat(offset)
                    .withFailMessage(() -> "Retried write did not return the offset of the record it wrote, got: " + offset)
                    .isZero();
        }
    }

    private static TransactionalConfigTopicWriter writer(Producer<String, String> producer) {
        return new TransactionalConfigTopicWriter(TOPIC, new RecordingProducers(producer), offsets());
    }

    private static MockProducer<String, String> autoCompletingProducer() {
        return new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
    }

    private static MockConsumer<String, String> offsets() {
        var consumer = new MockConsumer<String, String>("earliest");
        var partition = new TopicPartition(TOPIC, 0);
        consumer.updatePartitions(TOPIC, List.of(new PartitionInfo(TOPIC, 0, Node.noNode(), new Node[0], new Node[0])));
        consumer.assign(List.of(partition));
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        consumer.updateEndOffsets(Map.of(partition, 0L));
        return consumer;
    }

    private static final class RecordingProducers implements Supplier<Producer<String, String>> {

        private final Deque<Producer<String, String>> remaining;

        private final List<Producer<String, String>> handedOut = new ArrayList<>();

        @SafeVarargs
        RecordingProducers(Producer<String, String>... producers) {
            this.remaining = new ArrayDeque<>(List.of(producers));
        }

        @Override
        public Producer<String, String> get() {
            var producer = remaining.poll();
            if (producer == null) {
                throw new AssertionError("Writer asked for more producers than the test provided");
            }
            handedOut.add(producer);
            return producer;
        }

        List<Producer<String, String>> handedOut() {
            return handedOut;
        }
    }

    private static final class FailingSendProducer extends MockProducer<String, String> {

        private final RuntimeException failure;

        FailingSendProducer(RuntimeException failure) {
            super(true, null, new StringSerializer(), new StringSerializer());
            this.failure = failure;
        }

        @Override
        public synchronized Future<RecordMetadata> send(ProducerRecord<String, String> record) {
            return send(record, null);
        }

        @Override
        public synchronized Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
            if (callback != null) {
                callback.onCompletion(null, failure);
            }
            return CompletableFuture.failedFuture(failure);
        }
    }
}
