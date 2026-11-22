package io.jonasg.kawa.it;

import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.RenameFieldTransformationConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.awaitility.Awaitility.await;

/// End-to-end test for the `fail` policy on a transformed virtual topic: a poison record in the
/// middle of a batch stops the consumer at exactly that record, and seeking past it resumes.
class FailingTransformedVirtualTopicIT extends GatewayTestSupport {

    private static final long POISON_OFFSET = 2L;

    @Override
    protected Map<String, VirtualTopicConfig> filteredVirtualTopics() {
        return Map.of("payments", new VirtualTopicConfig("payments-json", null, false, new JsonFormatConfig(DecodeErrorPolicy.FAIL),
                List.of(new RenameFieldTransformationConfig("payment_id", "id")), null));
    }

    @Override
    protected List<NewTopic> initialTopics() {
        return List.of(new NewTopic("payments-json", 1, (short) 1));
    }

    @Override
    protected String groupId() {
        return "failing-transformed-virtual-topic-it";
    }

    @Test
    void stopsAtAPoisonRecordInTheMiddleOfABatchUntilTheConsumerSeeksPastIt() throws Exception {
        // given five payments on the physical topic in one batch, the third of which is not JSON
        produceInOneBatch(
                "{\"payment_id\":\"p-0\"}",
                "{\"payment_id\":\"p-1\"}",
                "not json",
                "{\"payment_id\":\"p-3\"}",
                "{\"payment_id\":\"p-4\"}");

        try (var consumer = consumer()) {
            consumer.subscribe(List.of("payments"));

            // when the consumer polls until the poison record stops it
            List<ConsumerRecord<String, String>> beforePoison = new ArrayList<>();
            var failure = new AtomicReference<KafkaException>();
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                try {
                    consumer.poll(Duration.ofMillis(200)).forEach(beforePoison::add);
                    return false;
                } catch (KafkaException e) {
                    failure.set(e);
                    return true;
                }
            });

            // then the payments before it arrive, and the failure names the poison record's offset
            assertThat(beforePoison).extracting(ConsumerRecord::value)
                    .withFailMessage(() -> "The payments before the poison record were not delivered transformed, got " + beforePoison)
                    .containsExactly("{\"id\":\"p-0\"}", "{\"id\":\"p-1\"}");
            assertThat(failure.get().getMessage())
                    .withFailMessage(() -> "The consumer's failure does not name the poison record's offset: " + failure.get().getMessage())
                    .contains("offset " + POISON_OFFSET);

            // when the consumer seeks past the poison record
            consumer.seek(new TopicPartition("payments", 0), POISON_OFFSET + 1);
            List<ConsumerRecord<String, String>> afterPoison = new ArrayList<>();
            eventually(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(afterPoison::add);
                assertThat(afterPoison).extracting(ConsumerRecord::offset)
                        .withFailMessage(() -> "The payments after the poison record did not arrive, got " + afterPoison)
                        .containsExactly(3L, 4L);
            });

            // then
            assertThat(afterPoison).extracting(ConsumerRecord::value)
                    .withFailMessage(() -> "The payments after the poison record were not delivered transformed, got " + afterPoison)
                    .containsExactly("{\"id\":\"p-3\"}", "{\"id\":\"p-4\"}");
        }
    }

    private static void produceInOneBatch(String... values) {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(LINGER_MS_CONFIG, 1_000);
        try (var producer = new KafkaProducer<String, String>(producerProps)) {
            for (String value : values) {
                producer.send(new ProducerRecord<>("payments-json", "k", value));
            }
            producer.flush();
        }
    }

    private KafkaConsumer<String, String> consumer() {
        Properties consumerProps = saslProps(gatewayBootstrap);
        consumerProps.put(GROUP_ID_CONFIG, groupId());
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(consumerProps);
    }
}
