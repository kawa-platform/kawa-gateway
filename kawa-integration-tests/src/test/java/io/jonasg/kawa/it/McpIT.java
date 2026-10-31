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

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
