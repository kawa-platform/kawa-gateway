package io.jonasg.kawa.it;

import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.RenameFieldTransformationConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/// End-to-end test for the `kawa-error` headers: under `onDecodeError: include`, a consumer of a
/// virtual topic learns why a record reached it unfiltered or untransformed, unless the filter or
/// the failing step turns the headers off.
class DecodeErrorHeadersVirtualTopicIT extends GatewayTestSupport {

    private static final JsonFormatConfig INCLUDE = new JsonFormatConfig(DecodeErrorPolicy.INCLUDE);
    private static final String NOT_JSON = "card=4111";
    private static final String WITH_LINES = "{\"status\":\"PAID\",\"lines\":[{\"sku\":\"A\"}]}";

    @Override
    protected Map<String, VirtualTopicConfig> filteredVirtualTopics() {
        return Map.of(
                "paid", new VirtualTopicConfig("orders-paid", new CelFilterConfig("value.status == 'PAID'"), false, INCLUDE),
                "paid-quiet", new VirtualTopicConfig("orders-paid-quiet", new CelFilterConfig("value.status == 'PAID'", false), false, INCLUDE),
                "renamed", new VirtualTopicConfig("orders-renamed", null, false, INCLUDE, List.of(
                        new RenameFieldTransformationConfig("status", "state"),
                        new RenameFieldTransformationConfig("lines.sku", "sku")), null),
                "renamed-quiet", new VirtualTopicConfig("orders-renamed-quiet", null, false, INCLUDE, List.of(
                        new RenameFieldTransformationConfig("status", "state"),
                        new RenameFieldTransformationConfig("lines.sku", "sku", false)), null));
    }

    @Override
    protected List<NewTopic> initialTopics() {
        return List.of(
                new NewTopic("orders-paid", 1, (short) 1),
                new NewTopic("orders-paid-quiet", 1, (short) 1),
                new NewTopic("orders-renamed", 1, (short) 1),
                new NewTopic("orders-renamed-quiet", 1, (short) 1));
    }

    @Override
    protected String groupId() {
        return "decode-error-headers-it";
    }

    @Test
    void tellsTheConsumerWhyARecordCameThroughUnfiltered() {
        // given a record the CEL filter cannot decode
        produce("orders-paid", "{\"status\":\"PAID\"}", NOT_JSON);

        // when
        List<ConsumerRecord<String, String>> records = consume("paid", 2);

        // then
        assertThat(headers(records.get(0)))
                .withFailMessage(() -> "A record the filter matched carries error headers: " + headers(records.get(0)))
                .isEmpty();
        assertThat(records.get(1).value()).isEqualTo(NOT_JSON);
        assertThat(headers(records.get(1)))
                .withFailMessage(() -> "The unfiltered record did not say why: " + headers(records.get(1)))
                .containsEntry("kawa-error-stage", "filter")
                .doesNotContainKey("kawa-error-step")
                .hasEntrySatisfying("kawa-error", reason -> assertThat(reason)
                        .startsWith("PayloadDecodeException: record value is not valid JSON")
                        .doesNotContain("4111"));
    }

    @Test
    void tellsTheConsumerWhyARecordCameThroughUntransformed() {
        // given a record whose lines are a list, which the second step cannot rename through
        produce("orders-renamed", "{\"status\":\"PAID\"}", WITH_LINES);

        // when
        List<ConsumerRecord<String, String>> records = consume("renamed", 2);

        // then
        assertThat(records.get(0).value()).isEqualTo("{\"state\":\"PAID\"}");
        assertThat(headers(records.get(0))).isEmpty();
        assertThat(records.get(1).value())
                .withFailMessage(() -> "The record was not delivered as stored: " + records.get(1).value())
                .isEqualTo(WITH_LINES);
        assertThat(headers(records.get(1)))
                .withFailMessage(() -> "The untransformed record did not name the failing step: " + headers(records.get(1)))
                .containsEntry("kawa-error-stage", "transformation")
                .containsEntry("kawa-error-step", "1")
                .hasEntrySatisfying("kawa-error", reason -> assertThat(reason).startsWith("PayloadMappingException: transformations[1] (renameField)"));
    }

    @Test
    void sendsNoErrorHeadersWhenTheFilterTurnsThemOff() {
        // given
        produce("orders-paid-quiet", NOT_JSON);

        // when
        List<ConsumerRecord<String, String>> records = consume("paid-quiet", 1);

        // then
        assertThat(headers(records.getFirst()))
                .withFailMessage(() -> "'errorHeader: false' on the filter still sent headers: " + headers(records.getFirst()))
                .isEmpty();
    }

    @Test
    void sendsNoErrorHeadersWhenTheFailingStepTurnsThemOff() {
        // given
        produce("orders-renamed-quiet", WITH_LINES);

        // when
        List<ConsumerRecord<String, String>> records = consume("renamed-quiet", 1);

        // then
        assertThat(records.getFirst().value()).isEqualTo(WITH_LINES);
        assertThat(headers(records.getFirst()))
                .withFailMessage(() -> "'errorHeader: false' on the failing step still sent headers: " + headers(records.getFirst()))
                .isEmpty();
    }

    private static void produce(String topic, String... values) {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(producerProps)) {
            for (String value : values) {
                producer.send(new ProducerRecord<>(topic, "k", value));
            }
            producer.flush();
        }
    }

    private List<ConsumerRecord<String, String>> consume(String virtualTopic, int count) {
        Properties consumerProps = saslProps(gatewayBootstrap);
        consumerProps.put(GROUP_ID_CONFIG, groupId() + "-" + virtualTopic);
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (var consumer = new KafkaConsumer<String, String>(consumerProps)) {
            consumer.subscribe(List.of(virtualTopic));
            eventually(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
                assertThat(records)
                        .withFailMessage(() -> "Expected " + count + " records from '" + virtualTopic + "', got " + records)
                        .hasSize(count);
            });
        }
        return records;
    }

    private static Map<String, String> headers(ConsumerRecord<String, String> record) {
        Map<String, String> headers = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }
        return headers;
    }
}
