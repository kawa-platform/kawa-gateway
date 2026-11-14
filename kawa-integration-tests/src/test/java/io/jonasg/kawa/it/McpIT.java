package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AdminConfig;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/// The gateway's MCP server, driven by the MCP SDK's own Streamable HTTP client against `/mcp` on the
/// admin port, so interop is checked against a real client rather than our reading of the spec.
class McpIT extends AdminHTTPTestSupport {

    private McpSyncClient client;

    @Override
    protected AdminConfig adminConfig() {
        return new AdminConfig(true, "127.0.0.1", 0, null);
    }

    @Override
    protected Map<String, String> virtualTopics() {
        return Map.of("orders", "orders-v2");
    }

    @Override
    protected List<NewTopic> initialTopics() {
        return List.of(new NewTopic("orders-v2", 1, (short) 1));
    }

    @BeforeEach
    void connect() {
        client = McpClient.sync(HttpClientStreamableHttpTransport.builder(baseUrl()).endpoint("/mcp").build())
                .requestTimeout(Duration.ofSeconds(10))
                .build();
        client.initialize();
    }

    /// Switches the mcp module back on, so a test that switched it off cannot break the others.
    /// Switching it on when it already is changes nothing.
    @AfterEach
    void reEnableMcpAndDisconnect() throws Exception {
        try {
            setMcpModule(true);
            eventually(() -> assertThat(mcpStatus())
                    .withFailMessage(() -> "/mcp was not switched back on by the mcp module")
                    .isEqualTo(200));
        } finally {
            client.closeGracefully();
        }
    }

    @Test
    void advertisesTheTools() {
        // when
        var tools = client.listTools().tools();

        // then
        assertThat(tools)
                .withFailMessage(() -> "MCP server did not advertise its tools: " + tools)
                .extracting(McpSchema.Tool::name)
                .contains("list_topics", "list_consumer_groups", "describe_consumer_group");
    }

    @Test
    void mcpModuleSwitchesTheEndpointOffThroughTheConfigTopic() throws Exception {
        // when
        var disableMcpResp = setMcpModule(false);

        // then
        assertThat(disableMcpResp.statusCode())
                .withFailMessage(() -> "PUT /modules/mcp was not accepted: " + disableMcpResp.body())
                .isEqualTo(200);
        eventually(() -> assertThat(mcpStatus())
                .withFailMessage(() -> "/mcp was not switched off by the mcp module")
                .isEqualTo(404));
    }

    @Test
    void listTopicsReturnsTheGatewaysTopics() {
        eventually(() -> {
            // when
            var result = client.callTool(new McpSchema.CallToolRequest("list_topics", Map.of()));

            // then
            assertThat(result.isError())
                    .withFailMessage(() -> "list_topics failed: " + result.content())
                    .isFalse();
            assertThatJson(result.structuredContent()).inPath("virtualTopics[?(@.name == 'orders')].physicalTopic")
                    .isArray().containsExactly("orders-v2");
            assertThatJson(result.structuredContent()).inPath("physicalTopics[*].name").isArray().contains("orders-v2");
            String text = ((McpSchema.TextContent) result.content().getFirst()).text();
            assertThatJson(text)
                    .withFailMessage(() -> "the text content is not the structured content as JSON: " + text)
                    .isEqualTo(result.structuredContent());
        });
    }

    @Test
    void consumerGroupToolsReportTheLagOfAGroupReadingAVirtualTopicAgainstItsPhysicalTopic() throws Exception {
        // given: a group that read three records of 'orders' and committed, then two more were produced
        String group = "mcp-it-orders-reader";
        produceToOrders(3);
        consumeAndCommit(group, 3);
        produceToOrders(2);

        eventually(() -> {
            // when
            var listResult = client.callTool(new McpSchema.CallToolRequest("list_consumer_groups", Map.of()));
            var describeResult = client.callTool(new McpSchema.CallToolRequest("describe_consumer_group",
                    Map.of("groupId", group)));

            // then
            assertThat(listResult.isError())
                    .withFailMessage(() -> "list_consumer_groups failed: " + listResult.content())
                    .isFalse();
            assertThatJson(listResult.structuredContent()).inPath("groups[?(@.groupId == '" + group + "')].totalLag")
                    .isArray().containsExactly(2);
            assertThat(describeResult.isError())
                    .withFailMessage(() -> "describe_consumer_group failed: " + describeResult.content())
                    .isFalse();
            assertThatJson(describeResult.structuredContent()).inPath("topics[0].physicalTopic").isEqualTo("orders-v2");
            assertThatJson(describeResult.structuredContent()).inPath("topics[0].virtualTopics[*].name")
                    .isArray().containsExactly("orders");
            assertThatJson(describeResult.structuredContent()).inPath("topics[0].partitions")
                    .isEqualTo("[{\"partition\":0,\"committedOffset\":3,\"endOffset\":5,\"lag\":2}]");
        });
    }

    @Test
    void describingAnUnknownConsumerGroupIsAToolErrorNamingIt() {
        // when
        var result = client.callTool(new McpSchema.CallToolRequest("describe_consumer_group", Map.of("groupId", "no-such-group")));

        // then
        assertThat(result.isError())
                .withFailMessage(() -> "describing an unknown group was not a tool error: " + result.content())
                .isTrue();
        assertThat(((McpSchema.TextContent) result.content().getFirst()).text()).isEqualTo("consumer group 'no-such-group' not found");
    }

    private static void produceToOrders(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            gatewayProducer.send(new ProducerRecord<>("orders", "key-" + i, "value-" + i)).get();
        }
    }

    /// Reads `count` records of the virtual topic `orders` through the gateway as `group`, commits,
    /// and leaves the group.
    private static void consumeAndCommit(String group, int count) {
        Properties props = saslProps(gatewayBootstrap);
        props.put(GROUP_ID_CONFIG, group);
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of("orders"));
            List<String> received = new ArrayList<>();
            eventually(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> received.add(record.value()));
                assertThat(received)
                        .withFailMessage(() -> "group " + group + " did not receive " + count + " records of 'orders': " + received)
                        .hasSize(count);
            });
            consumer.commitSync();
        }
    }

    private HttpResponse<String> setMcpModule(boolean enabled) throws Exception {
        return httpExec(reqBuilder("/modules/mcp")
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString("{\"enabled\":" + enabled + "}"))
                .build());
    }

    private int mcpStatus() throws Exception {
        return httpExec(reqBuilder("/mcp")
                .header("Content-Type", "application/json")
                .header("Accept", "application/json, text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}"))
                .build()).statusCode();
    }
}
