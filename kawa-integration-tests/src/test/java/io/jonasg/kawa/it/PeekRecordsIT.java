package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.ModuleConfig;
import io.jonasg.kawa.config.ModuleId;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/// `peek_records` against a real broker, through the MCP SDK's client: offsets and timestamps are resolved by
/// Kafka itself, and inspection leaves no trace in the cluster's consumer groups.
class PeekRecordsIT extends AdminHTTPTestSupport {

    private static final String TOPIC = "peek-events";
    private static final Instant T0 = Instant.parse("2026-11-25T10:00:00Z");

    private McpSyncClient client;

    @Override
    protected AdminConfig adminConfig() {
        return new AdminConfig(true, "127.0.0.1", 0, null, null, "https://kawa.example.com");
    }

    @Override
    protected List<ModuleConfig> modules() {
        return List.of(new ModuleConfig(ModuleId.MCP, true));
    }

    @Override
    protected List<NewTopic> initialTopics() {
        return List.of(new NewTopic(TOPIC, 3, (short) 1));
    }

    /// Ten records per partition, the n-th at `T0 + n minutes` with value `{"n": n, "partition": p}`.
    @BeforeAll
    static void produceRecords() throws Exception {
        try (var producer = new KafkaProducer<String, String>(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap,
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
            for (int partition = 0; partition < 3; partition++) {
                for (int n = 0; n < 10; n++) {
                    producer.send(new ProducerRecord<>(TOPIC, partition, T0.plusSeconds(60L * n).toEpochMilli(),
                            "key-" + partition + "-" + n, "{\"n\":" + n + ",\"partition\":" + partition + "}")).get();
                }
            }
        }
    }

    @BeforeEach
    void connect() {
        client = McpClient.sync(HttpClientStreamableHttpTransport.builder(baseUrl()).endpoint("/mcp").build())
                .requestTimeout(Duration.ofSeconds(30))
                .build();
        client.initialize();
    }

    @AfterEach
    void disconnect() {
        client.closeGracefully();
    }

    @Test
    void readsTheLatestRecordsOfEveryPartition() {
        // when
        var result = peek(Map.of("topic", TOPIC, "limit", 6));

        // then
        assertThatJson(result).inPath("ranges[*].logEndOffset").isArray().containsExactly(10, 10, 10);
        assertThatJson(result).inPath("records[*].offset").isArray()
                .withFailMessage(() -> "not the latest two records of each partition were read: " + result)
                .containsExactly(8, 8, 8, 9, 9, 9);
        assertThatJson(result).inPath("records[*].partition").isArray().containsExactly(0, 1, 2, 0, 1, 2);
        assertThatJson(result).inPath("limitReached").isEqualTo(true);
    }

    @Test
    void resolvesATimeRangeToOffsetsWithTheBroker() {
        // when
        var result = peek(Map.of("topic", TOPIC, "partitions", List.of(1), "start_time", "2026-11-25T10:03:00Z",
                "until", "2026-11-25T10:06:00Z", "include_values", true));

        // then
        assertThatJson(result).inPath("ranges[0].rangeStartOffset").isEqualTo(3);
        assertThatJson(result).inPath("ranges[0].rangeEndOffset").isEqualTo(6);
        assertThatJson(result).inPath("records[*].value.n").isArray()
                .withFailMessage(() -> "the time range was not read: " + result).containsExactly(3, 4, 5);
        assertThatJson(result).inPath("exhaustive").isEqualTo(true);
    }

    @Test
    void searchesValuesAcrossPartitions() {
        // when
        var result = peek(Map.of("topic", TOPIC, "value_contains", "7", "start_offsets", Map.of("0", 0, "2", 5)));

        // then
        assertThatJson(result).inPath("records[*].key").isArray()
                .withFailMessage(() -> "the matching records were not found: " + result).containsExactly("key-0-7", "key-2-7");
        assertThatJson(result).inPath("exhaustive").isEqualTo(true);
    }

    @Test
    void leavesNoConsumerGroupBehind() throws Exception {
        // given
        Set<String> before = groups();

        // when
        peek(Map.of("topic", TOPIC, "start_offsets", Map.of("0", 0, "1", 0, "2", 0)));
        peek(Map.of("topic", TOPIC, "key_contains", "key-1"));

        // then
        Set<String> after = groups();
        assertThat(after).withFailMessage(() -> "peek_records created consumer groups: before " + before + ", after " + after)
                .isEqualTo(before);
    }

    private Object peek(Map<String, Object> arguments) {
        var result = client.callTool(new McpSchema.CallToolRequest("peek_records", arguments));
        assertThat(result.isError()).withFailMessage(() -> "peek_records failed: " + result.content()).isFalse();
        return result.structuredContent();
    }

    private static Set<String> groups() throws Exception {
        return brokerAdmin.listGroups().all().get().stream().map(GroupListing::groupId).collect(Collectors.toSet());
    }
}
