package io.jonasg.kawa.it;

import io.jonasg.kawa.config.ConvertTransformationConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.DropFieldsTransformationConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.RenameFieldTransformationConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.config.XmlFormatConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end test for transformations on the fetch path: JSON orders are produced to the physical
/// topic, and a consumer group member of the virtual topic receives them through the gateway as
/// XML, with the mapping applied - over compressed batches and the consumer's fetch session.
class TransformedVirtualTopicIT extends GatewayTestSupport {

    @Override
    protected Map<String, VirtualTopicConfig> filteredVirtualTopics() {
        return Map.of("orders-xml", new VirtualTopicConfig("orders-json", null, false, new JsonFormatConfig(DecodeErrorPolicy.SKIP),
                List.of(new DropFieldsTransformationConfig(List.of("internal_risk_score")),
                        new RenameFieldTransformationConfig("order_id", "id"),
                        new ConvertTransformationConfig(new XmlFormatConfig(DecodeErrorPolicy.SKIP, "order"))),
                null));
    }

    @Override
    protected List<NewTopic> initialTopics() {
        return List.of(new NewTopic("orders-json", 1, (short) 1));
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
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline && received.stream().noneMatch(record -> record.key().equals("o-2"))) {
                consumer.poll(Duration.ofMillis(200)).forEach(received::add);
            }
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
}
