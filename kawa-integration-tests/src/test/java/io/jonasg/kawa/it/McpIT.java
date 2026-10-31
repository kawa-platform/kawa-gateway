package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AdminConfig;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
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

    @AfterEach
    void disconnect() {
        client.closeGracefully();
    }

    @Test
    void advertisesTheListTopicsTool() {
        // when
        var tools = client.listTools().tools();

        // then
        assertThat(tools)
                .withFailMessage(() -> "MCP server did not advertise list_topics: " + tools)
                .extracting(McpSchema.Tool::name)
                .contains("list_topics");
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
            String text = ((McpSchema.TextContent) result.content().getFirst()).text();
            assertThatJson(text).inPath("[?(@.name == 'orders')].physicalTopic").isArray().containsExactly("orders-v2");
        });
    }
}
