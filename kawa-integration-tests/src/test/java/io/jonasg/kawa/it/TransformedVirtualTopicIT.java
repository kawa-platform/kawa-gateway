package io.jonasg.kawa.it;

import io.jonasg.kawa.config.ConvertTransformationConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.DropFieldsTransformationConfig;
import io.jonasg.kawa.config.DropHeadersTransformationConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.RenameFieldTransformationConfig;
import io.jonasg.kawa.config.SetHeaderTransformationConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.config.XmlFormatConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/// End-to-end test for transformations on the fetch path: JSON orders are produced to the physical
/// topic, and a consumer group member of the virtual topic receives them through the gateway as
/// XML, with the mapping applied - over compressed batches and the consumer's fetch session. Header
/// steps make the format headers of a converted record describe the XML it is delivered as.
class TransformedVirtualTopicIT extends GatewayTestSupport {

    @Override
    protected Map<String, VirtualTopicConfig> filteredVirtualTopics() {
        return Map.of(
                "orders-xml", new VirtualTopicConfig("orders-json", null, false, new JsonFormatConfig(DecodeErrorPolicy.SKIP),
                        List.of(new DropFieldsTransformationConfig(List.of("internal_risk_score")),
                                new RenameFieldTransformationConfig("order_id", "id"),
                                new ConvertTransformationConfig(new XmlFormatConfig(DecodeErrorPolicy.SKIP, "order"))),
                        null),
                "invoices-xml", new VirtualTopicConfig("invoices-json", null, false, new JsonFormatConfig(DecodeErrorPolicy.INCLUDE),
                        List.of(new ConvertTransformationConfig(new XmlFormatConfig(DecodeErrorPolicy.SKIP, "invoice")),
                                new DropHeadersTransformationConfig(List.of("__TypeId__")),
                                new SetHeaderTransformationConfig("content-type", "application/xml")),
                        null),
                "events-clean", new VirtualTopicConfig("events-raw", null, false, null,
                        List.of(new DropHeadersTransformationConfig(List.of("__TypeId__"))), null));
    }

    @Override
    protected List<NewTopic> initialTopics() {
        return List.of(new NewTopic("orders-json", 1, (short) 1), new NewTopic("invoices-json", 1, (short) 1),
                new NewTopic("events-raw", 1, (short) 1));
    }

    @Override
    protected String groupId() {
        return "transformed-virtual-topic-it";
    }

    @Test
    void consumesJsonProducedOrdersAsMappedXml() throws Exception {
        // given JSON orders on the physical topic in one lz4 batch, plus a record that is not JSON
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(COMPRESSION_TYPE_CONFIG, "lz4");
        producerProps.put(LINGER_MS_CONFIG, 50);
        try (var producer = new KafkaProducer<String, String>(producerProps)) {
            producer.send(new ProducerRecord<>("orders-json", "o-1", "{\"order_id\":\"o-1\",\"status\":\"PAID\",\"internal_risk_score\":7}"));
            producer.send(new ProducerRecord<>("orders-json", "broken", "not json"));
            producer.send(new ProducerRecord<>("orders-json", "o-2", "{\"order_id\":\"o-2\",\"status\":\"OPEN\",\"internal_risk_score\":2}"));
            producer.flush();
        }

        // when a group member consumes the virtual topic through the gateway until the last order arrives
        List<ConsumerRecord<String, String>> received = new ArrayList<>();
        Properties consumerProps = saslProps(gatewayBootstrap);
        consumerProps.put(GROUP_ID_CONFIG, groupId());
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (var consumer = new KafkaConsumer<String, String>(consumerProps)) {
            consumer.subscribe(List.of("orders-xml"));
            eventually(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(received::add);
                assertThat(received).extracting(ConsumerRecord::key)
                        .withFailMessage(() -> "The order 'o-2' was not delivered, got keys " + received.stream().map(ConsumerRecord::key).toList())
                        .contains("o-2");
            });
        }

        // then both orders arrive as XML without the risk score; the record that is not JSON is skipped
        List<String> values = received.stream().map(ConsumerRecord::value).toList();
        assertThat(values)
                .withFailMessage(() -> "The orders were not delivered as mapped XML, got " + values)
                .containsExactly("<order><id>o-1</id><status>PAID</status></order>", "<order><id>o-2</id><status>OPEN</status></order>");
        assertThat(received).extracting(ConsumerRecord::offset)
                .withFailMessage(() -> "The delivered orders did not keep their offsets")
                .containsExactly(0L, 2L);
    }

    @Test
    void consumesHeadersDescribingTheConvertedFormatWhileTheStoredRecordKeepsItsOwn() throws Exception {
        // given a JSON invoice and a value that is not JSON, produced through the gateway with JSON format headers
        List<Header> jsonHeaders = List.of(header("content-type", "application/json"), header("__TypeId__", "com.acme.Invoice"),
                header("region", "eu"));
        Properties producerProps = saslProps(gatewayBootstrap);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(producerProps)) {
            producer.send(new ProducerRecord<>("invoices-xml", null, "i-1", "{\"id\":\"i-1\"}", jsonHeaders)).get();
            producer.send(new ProducerRecord<>("invoices-xml", null, "broken", "not json", jsonHeaders)).get();
        }

        // when a consumer reads the virtual topic through the gateway, and another the physical topic from the broker
        List<ConsumerRecord<String, String>> virtual = consume(gatewayBootstrap, "invoices-xml", true, 2);
        List<ConsumerRecord<String, String>> stored = consume(brokerBootstrap, "invoices-json", false, 2);

        // then the transformed invoice's headers describe its XML value
        assertThat(virtual.getFirst().value())
                .withFailMessage(() -> "The invoice was not delivered as XML, got " + virtual.getFirst().value())
                .isEqualTo("<invoice><id>i-1</id></invoice>");
        assertThat(List.of(virtual.getFirst().headers().toArray()))
                .withFailMessage(() -> "The transformed invoice's headers do not describe XML: " + virtual.getFirst().headers())
                .containsExactly(header("region", "eu"), header("content-type", "application/xml"));
        // and the value delivered untransformed under include keeps the headers that describe it
        assertThat(List.of(virtual.getLast().headers().toArray()))
                .withFailMessage(() -> "The untransformed value's headers were rewritten: " + virtual.getLast().headers())
                .containsExactlyElementsOf(jsonHeaders);
        // and the produce path stored the records as sent
        assertThat(stored).extracting(ConsumerRecord::value)
                .withFailMessage(() -> "The stored values were changed on produce: " + stored.stream().map(ConsumerRecord::value).toList())
                .containsExactly("{\"id\":\"i-1\"}", "not json");
        assertThat(stored).allSatisfy(record -> assertThat(List.of(record.headers().toArray()))
                .withFailMessage(() -> "The stored headers were changed on produce: " + record.headers())
                .containsExactlyElementsOf(jsonHeaders));
    }

    @Test
    void passesTheValueBytesOfAHeaderOnlyTopicThroughAndDropsItsHeader() throws Exception {
        // given a value that is not JSON, produced to the physical topic with a header the topic drops
        String value = "{ not  json, but kept as is }";
        Properties producerProps = saslProps(gatewayBootstrap);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(producerProps)) {
            producer.send(new ProducerRecord<>("events-clean", null, "e-1", value,
                    List.of(header("__TypeId__", "com.acme.Event"), header("region", "eu")))).get();
        }

        // when a consumer reads the header-only virtual topic through the gateway
        List<ConsumerRecord<String, String>> received = consume(gatewayBootstrap, "events-clean", true, 1);

        // then the value is delivered byte for byte and the dropped header is gone
        assertThat(received.getFirst().value())
                .withFailMessage(() -> "The value of a header-only topic was changed: " + received.getFirst().value())
                .isEqualTo(value);
        assertThat(List.of(received.getFirst().headers().toArray()))
                .withFailMessage(() -> "The header-only topic did not drop __TypeId__: " + received.getFirst().headers())
                .containsExactly(header("region", "eu"));
    }

    /// The first `count` records of `topic`, read from offset 0 at `bootstrap`.
    private List<ConsumerRecord<String, String>> consume(String bootstrap, String topic, boolean sasl, int count) {
        Properties consumerProps = sasl ? saslProps(bootstrap) : new Properties();
        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consumerProps.put(GROUP_ID_CONFIG, groupId() + "-" + topic);
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> received = new ArrayList<>();
        try (var consumer = new KafkaConsumer<String, String>(consumerProps)) {
            consumer.subscribe(List.of(topic));
            eventually(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(received::add);
                assertThat(received)
                        .withFailMessage(() -> count + " records of '" + topic + "' were not delivered, got " + received.size())
                        .hasSizeGreaterThanOrEqualTo(count);
            });
        }
        return received.subList(0, count);
    }

    private static Header header(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }
}
