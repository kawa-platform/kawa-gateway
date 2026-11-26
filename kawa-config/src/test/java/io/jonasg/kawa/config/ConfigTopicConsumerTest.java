package io.jonasg.kawa.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigTopicConsumerTest {

    private static final String TOPIC = "kawa-config";

    @Test
    void appliesOnlyLatestRetainedSnapshotDuringInitialLoad() throws InterruptedException {
        // given
        var records = mockConsumer(2);
        records.schedulePollTask(() ->
                records.addRecord(record(0, "{\"virtualTopics\":{\"orders\":\"orders-v1\"}}")));
        records.schedulePollTask(() ->
                records.addRecord(record(1, "{\"virtualTopics\":{\"orders\":\"orders-v2\"}}")));
        List<DynamicConfig> received = new ArrayList<>();
        var consumer = new ConfigTopicConsumer(TOPIC, records,
                (config, _) -> received.add(config), _ -> { });

        try {
            // when
            consumer.start();
            consumer.awaitInitialLoad();

            // then
            assertThat(received)
                    .withFailMessage(() -> "Initial load applied superseded config snapshots: " + received)
                    .singleElement()
                    .extracting(DynamicConfig::virtualTopics)
                    .satisfies(virtualTopics -> assertThat(virtualTopics)
                            .containsEntry("orders", new VirtualTopicConfig("orders-v2")));
        } finally {
            consumer.close();
        }
    }

    @Test
    void ignoresInvalidSupersededSnapshotDuringInitialLoad() throws InterruptedException {
        // given
        var records = mockConsumer(2);
        records.schedulePollTask(() -> {
            records.addRecord(record(0, "not-json"));
            records.addRecord(record(1, "{\"virtualTopics\":{\"orders\":\"orders-v2\"}}"));
        });
        List<DynamicConfig> received = new ArrayList<>();
        var consumer = new ConfigTopicConsumer(TOPIC, records,
                (config, _) -> received.add(config), _ -> { });

        try {
            // when
            consumer.start();
            consumer.awaitInitialLoad();

            // then
            assertThat(received)
                    .withFailMessage(() -> "An invalid superseded snapshot prevented the latest snapshot from loading")
                    .singleElement()
                    .extracting(DynamicConfig::virtualTopics)
                    .satisfies(virtualTopics -> assertThat(virtualTopics)
                            .containsEntry("orders", new VirtualTopicConfig("orders-v2")));
        } finally {
            consumer.close();
        }
    }

    @Test
    void failsInitialLoadWhenLatestSnapshotIsInvalid() {
        // given
        var records = mockConsumer(2);
        records.schedulePollTask(() -> {
            records.addRecord(record(0, "{\"virtualTopics\":{\"orders\":\"orders-v1\"}}"));
            records.addRecord(record(1, "not-json"));
        });
        List<DynamicConfig> received = new ArrayList<>();
        var consumer = new ConfigTopicConsumer(TOPIC, records,
                (config, _) -> received.add(config), _ -> { });

        try {
            // when
            consumer.start();

            // then
            assertThatThrownBy(consumer::awaitInitialLoad)
                    .withFailMessage(() -> "Initial load did not fail for the invalid authoritative snapshot")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Cannot read config topic")
                    .hasRootCauseInstanceOf(tools.jackson.core.JacksonException.class);
            assertThat(received)
                    .withFailMessage(() -> "Initial load fell back to an older valid snapshot")
                    .isEmpty();
        } finally {
            consumer.close();
        }
    }

    @Test
    void failsInitialLoadWhenLatestSnapshotIsRejected() {
        // given
        var records = mockConsumer(1);
        records.schedulePollTask(() -> records.addRecord(record(0, "{}")));
        var consumer = new ConfigTopicConsumer(TOPIC, records,
                (_, _) -> {
                    throw new IllegalArgumentException("rejected");
                }, _ -> { });

        try {
            // when
            consumer.start();

            // then
            assertThatThrownBy(consumer::awaitInitialLoad)
                    .withFailMessage(() -> "Initial load did not fail when the authoritative snapshot was rejected")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Cannot read config topic")
                    .hasRootCauseInstanceOf(IllegalArgumentException.class)
                    .hasRootCauseMessage("rejected");
        } finally {
            consumer.close();
        }
    }

    @Test
    void completesInitialLoadWhenTopicIsEmpty() throws InterruptedException {
        // given
        var records = mockConsumer(0);
        List<DynamicConfig> received = new ArrayList<>();
        var consumer = new ConfigTopicConsumer(TOPIC, records,
                (config, _) -> received.add(config), _ -> { });

        try {
            // when
            consumer.start();
            consumer.awaitInitialLoad();

            // then
            assertThat(received)
                    .withFailMessage(() -> "An empty config topic unexpectedly applied a snapshot")
                    .isEmpty();
        } finally {
            consumer.close();
        }
    }

    @Test
    void deliversValidSnapshotToCallback() {
        // given
        List<DynamicConfig> received = new ArrayList<>();
        ConfigTopicConsumer consumer = new ConfigTopicConsumer(
                "localhost:9092", TOPIC, received::add);

        // when
        consumer.handle(record("""
                {"virtualTopics":{"orders":"orders-v2"}}
                """));

        // then
        assertThat(received).hasSize(1);
        assertThat(received.getFirst().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"));
    }

    @Test
    void skipsUnparseableMessageWithoutInvokingCallback() {
        // given
        AtomicInteger calls = new AtomicInteger();
        ConfigTopicConsumer consumer = new ConfigTopicConsumer(
                "localhost:9092", TOPIC, _ -> calls.incrementAndGet());

        // when
        assertThatCode(() -> consumer.handle(record("not-json")))
                .doesNotThrowAnyException();

        // then
        assertThat(calls).hasValue(0);
    }

    @Test
    void skipsMessageRejectedByCallbackWithoutKillingConsumer() {
        // given
        AtomicInteger calls = new AtomicInteger();
        ConfigTopicConsumer consumer = new ConfigTopicConsumer(
                "localhost:9092", TOPIC, _ -> {
            calls.incrementAndGet();
            throw new IllegalArgumentException("rejected");
        });

        // when
        assertThatCode(() -> consumer.handle(record("{}")))
                .doesNotThrowAnyException();

        // then
        assertThat(calls).hasValue(1);
    }

    @Test
    void appliesDefaultsToPartialSnapshot() {
        // given
        List<DynamicConfig> received = new ArrayList<>();
        ConfigTopicConsumer consumer = new ConfigTopicConsumer(
                "localhost:9092", TOPIC, received::add);

        // when
        consumer.handle(record("{\"listeners\":[{\"port\":9092}]}"));

        // then
        assertThat(received).hasSize(1);
        assertThat(received.getFirst().virtualTopics()).isEmpty();
    }

    @Test
    void deliversSnapshotIgnoringUnknownKeysFromLegacySnapshots() {
        // given
        List<DynamicConfig> received = new ArrayList<>();
        ConfigTopicConsumer consumer = new ConfigTopicConsumer(
                "localhost:9092", TOPIC, received::add);

        // when
        consumer.handle(record(
                "{\"name\":\"old\",\"metrics\":{\"enabled\":false,\"prometheusPort\":0},"
                + "\"virtualTopics\":{\"orders\":\"orders-v2\"}}"));

        // then
        assertThat(received).hasSize(1);
        assertThat(received.getFirst().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"));
    }

    private static ConsumerRecord<String, String> record(String value) {
        return record(0, value);
    }

    private static ConsumerRecord<String, String> record(long offset, String value) {
        return new ConsumerRecord<>(TOPIC, 0, offset, "config", value);
    }

    private static MockConsumer<String, String> mockConsumer(long endOffset) {
        var records = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var partition = new TopicPartition(TOPIC, 0);
        records.updateBeginningOffsets(Map.of(partition, 0L));
        records.updateEndOffsets(Map.of(partition, endOffset));
        return records;
    }
}
