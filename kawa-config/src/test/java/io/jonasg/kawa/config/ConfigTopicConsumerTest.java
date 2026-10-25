package io.jonasg.kawa.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ConfigTopicConsumerTest {

    private static final String TOPIC = "kawa-config";

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
        return new ConsumerRecord<>(TOPIC, 0, 0L, "config", value);
    }
}
