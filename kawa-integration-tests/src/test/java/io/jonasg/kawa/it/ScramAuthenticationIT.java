package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end check that real Kafka clients authenticate to the gateway with
/// SCRAM-SHA-256 and SCRAM-SHA-512 (via the standard `ScramLoginModule`) and can
/// produce and consume through it.
class ScramAuthenticationIT extends GatewayTestSupport {

    private static final String TOPIC = "scram-round-trip";

    @Override
    protected AuthConfig authConfig() {
        return new AuthConfig(
                Set.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"),
                Map.of(
                        DEFAULT_PRINCIPAL, client("PLAIN", DEFAULT_PASSWORD),
                        "scram256", client("SCRAM-SHA-256", "secret256"),
                        "scram512", client("SCRAM-SHA-512", "secret512")),
                null);
    }

    @Override
    protected RbacConfig rbacConfig() {
        var role = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, TOPIC, PatternType.LITERAL), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.GROUP, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.TRANSACTIONAL_ID, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.CLUSTER, null), AclOperation.ALL)));
        return new RbacConfig(
                Map.of("scram-users", role),
                Map.of("scram-group",
                        new GroupConfig(List.of(DEFAULT_PRINCIPAL, "scram256", "scram512"), List.of("scram-users"))));
    }

    @Override
    protected List<NewTopic> initialTopics() {
        return List.of(new NewTopic(TOPIC, 1, (short) 1));
    }

    @Test
    void scramSha256ProducerConsumerRoundTrip() throws Exception {
        // given
        Properties producerProps = scramSaslProps(gatewayBootstrap, "SCRAM-SHA-256", "scram256", "secret256");
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // when
        try (var producer = new KafkaProducer<String, String>(producerProps)) {
            producer.send(new ProducerRecord<>(TOPIC, "k1", "v1")).get(5, TimeUnit.SECONDS);
        }

        // then
        Properties consumerProps = scramSaslProps(gatewayBootstrap, "SCRAM-SHA-256", "scram256", "secret256");
        consumerProps.put(GROUP_ID_CONFIG, "scram-256-it");
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (var consumer = new KafkaConsumer<String, String>(consumerProps)) {
            consumer.subscribe(List.of(TOPIC));
            ConsumerRecord<String, String> record = pollForValue(consumer, "v1");
            assertThat(record).withFailMessage(() -> "SCRAM-SHA-256 client did not receive the produced record")
                    .isNotNull();
            assertThat(record.value()).isEqualTo("v1");
        }
    }

    @Test
    void scramSha512ProducerConsumerRoundTrip() throws Exception {
        // given
        Properties producerProps = scramSaslProps(gatewayBootstrap, "SCRAM-SHA-512", "scram512", "secret512");
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // when
        try (var producer = new KafkaProducer<String, String>(producerProps)) {
            producer.send(new ProducerRecord<>(TOPIC, "k2", "v2")).get(5, TimeUnit.SECONDS);
        }

        // then
        Properties consumerProps = scramSaslProps(gatewayBootstrap, "SCRAM-SHA-512", "scram512", "secret512");
        consumerProps.put(GROUP_ID_CONFIG, "scram-512-it");
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (var consumer = new KafkaConsumer<String, String>(consumerProps)) {
            consumer.subscribe(List.of(TOPIC));
            ConsumerRecord<String, String> record = pollForValue(consumer, "v2");
            assertThat(record).withFailMessage(() -> "SCRAM-SHA-512 client did not receive the produced record")
                    .isNotNull();
            assertThat(record.value()).isEqualTo("v2");
        }
    }

    private ConsumerRecord<String, String> pollForValue(KafkaConsumer<String, String> consumer, String value) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(2))) {
                if (value.equals(record.value())) {
                    return record;
                }
            }
        }
        return null;
    }
}
