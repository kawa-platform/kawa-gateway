package io.jonasg.kawa.it;

import com.sun.net.httpserver.HttpServer;
import io.jonasg.kawa.config.AvroFormatConfig;
import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.config.SchemaRegistryConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end test for CEL consume filters over Avro record values: records in the schema
/// registry wire format are written to the physical topic, and a consumer of the virtual topic
/// through the gateway only receives those whose decoded value matches. The schema registry is a
/// stub on the JDK's [HttpServer].
class AvroVirtualTopicIT extends GatewayTestSupport {

    private static final int SCHEMA_ID = 1;

    private static final Schema ORDER = new Schema.Parser().parse("""
            {"type": "record", "name": "Order", "fields": [
              {"name": "status", "type": "string"},
              {"name": "amount", "type": "double"}
            ]}""");

    private final AtomicInteger schemaLookups = new AtomicInteger();
    private final HttpServer registry = startRegistry();

    @Override
    protected ClusterConfig clusterConfig() {
        String url = "http://127.0.0.1:" + registry.getAddress().getPort();
        return new ClusterConfig(List.of(brokerBootstrap), null, new SchemaRegistryConfig(url, null, null));
    }

    @Override
    protected Map<String, VirtualTopicConfig> filteredVirtualTopics() {
        return Map.of("paid-orders", new VirtualTopicConfig("orders-avro",
                new CelFilterConfig("value.status == \"PAID\" && value.amount > 100"), false, new AvroFormatConfig()));
    }

    @Override
    protected List<NewTopic> initialTopics() {
        return List.of(new NewTopic("orders-avro", 1, (short) 1));
    }

    @Override
    protected String groupId() {
        return "avro-virtual-topic-it";
    }

    @AfterAll
    void stopRegistry() {
        registry.stop(0);
    }

    @Test
    void deliversOnlyAvroRecordsWhoseDecodedValueMatchesTheFilter() throws Exception {
        // given orders on the physical topic, plus a record that is not Avro
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (var producer = new KafkaProducer<String, byte[]>(producerProps)) {
            send(producer, "paid-big", order("PAID", 250.0));
            send(producer, "open-big", order("OPEN", 300.0));
            send(producer, "paid-small", order("PAID", 20.0));
            send(producer, "not-avro", "{\"status\":\"PAID\"}".getBytes(StandardCharsets.UTF_8));
            send(producer, "paid-last", order("PAID", 101.5));
        }

        // when consuming the virtual topic through the gateway until the last order arrives
        List<String> keys = new ArrayList<>();
        Properties consumerProps = saslProps(gatewayBootstrap);
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        try (var consumer = new KafkaConsumer<String, byte[]>(consumerProps)) {
            var virtual = new TopicPartition("paid-orders", 0);
            consumer.assign(List.of(virtual));
            consumer.seekToBeginning(List.of(virtual));
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline && !keys.contains("paid-last")) {
                consumer.poll(Duration.ofMillis(200)).forEach(record -> keys.add(record.key()));
            }
        }

        // then only paid orders over 100 are delivered; the non-Avro record is skipped
        assertThat(keys)
                .withFailMessage(() -> "Only the paid orders over 100 were expected through the gateway, got " + keys)
                .containsExactly("paid-big", "paid-last");
        assertThat(schemaLookups.get())
                .withFailMessage(() -> "Schema id " + SCHEMA_ID + " was not looked up exactly once, lookups: " + schemaLookups.get())
                .isEqualTo(1);
    }

    private static void send(KafkaProducer<String, byte[]> producer, String key, byte[] value) throws Exception {
        producer.send(new ProducerRecord<>("orders-avro", 0, key, value)).get(5, TimeUnit.SECONDS);
    }

    /// An order in the schema registry wire format: magic byte, schema id, Avro binary.
    private static byte[] order(String status, double amount) {
        var order = new GenericData.Record(ORDER);
        order.put("status", status);
        order.put("amount", amount);
        var out = new ByteArrayOutputStream();
        out.write(0);
        out.writeBytes(ByteBuffer.allocate(4).putInt(SCHEMA_ID).array());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            new GenericDatumWriter<GenericData.Record>(ORDER).write(order, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    /// Serves `GET /schemas/ids/1` like a Confluent schema registry; any other id is a 404.
    private HttpServer startRegistry() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/schemas/ids/", exchange -> {
                boolean known = exchange.getRequestURI().getPath().equals("/schemas/ids/" + SCHEMA_ID);
                if (known) {
                    schemaLookups.incrementAndGet();
                }
                String body = known
                        ? "{\"schema\":" + quote(ORDER.toString()) + "}"
                        : "{\"error_code\":40403,\"message\":\"Schema not found\"}";
                byte[] response = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(known ? 200 : 404, response.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response);
                }
            });
            server.start();
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String quote(String json) {
        return "\"" + json.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
