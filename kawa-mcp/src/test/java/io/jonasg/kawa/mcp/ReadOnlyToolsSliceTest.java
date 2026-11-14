package io.jonasg.kawa.mcp;

import io.jonasg.kawa.core.module.ModuleRegistry;
import io.jonasg.kawa.http.web.Router;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.server.McpStatelessServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import io.modelcontextprotocol.spec.McpSchema.ToolAnnotations;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThatThrownBy;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// The MCP server only serves tools that read: every tool is annotated read-only, and a tool that is
/// not is refused at startup. The tool list is pinned here, so adding a tool is a deliberate change.
class ReadOnlyToolsSliceTest extends McpSliceTestBase {

    @Test
    void servesOnlyTheKnownReadOnlyTools() throws Exception {
        // given
        startServer();

        // when
        var toolsResp = rpc("tools/list", "{}");

        // then
        assertThatJson(toolsResp.body()).inPath("result.tools[*].name").isArray()
                .withFailMessage(() -> "the MCP tool list changed; a new tool must only read gateway or broker state "
                        + "and carry ToolResults.READ_ONLY: " + toolsResp.body())
                .containsExactlyInAnyOrder(ListTopicsTool.NAME, ListConsumerGroupsTool.NAME, DescribeConsumerGroupTool.NAME);
        assertThatJson(toolsResp.body()).inPath("result.tools[*].annotations.readOnlyHint").isArray()
                .withFailMessage(() -> "not every MCP tool is advertised as read-only: " + toolsResp.body())
                .containsOnly(true)
                .hasSize(3);
        assertThatJson(toolsResp.body()).inPath("result.tools[*].annotations.destructiveHint").isArray()
                .withFailMessage(() -> "an MCP tool is advertised as destructive: " + toolsResp.body())
                .containsOnly(false)
                .hasSize(3);
    }

    @Test
    void refusesToServeAToolThatIsNotAnnotatedReadOnly() {
        // given
        var writer = tool("delete_topic", ToolAnnotations.builder().readOnlyHint(false).destructiveHint(true).build());

        // when / then
        assertThatThrownBy(() -> new KawaMcpServer(new Router(), new ModuleRegistry(), null, McpJsonDefaults.getMapper(),
                List.of(writer)))
                .withFailMessage(() -> "a tool annotated readOnlyHint=false was served")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("delete_topic");
    }

    @Test
    void refusesToServeAToolWithoutAnnotations() {
        // given
        var unannotated = tool("whatever", null);

        // when / then
        assertThatThrownBy(() -> new KawaMcpServer(new Router(), new ModuleRegistry(), null, McpJsonDefaults.getMapper(),
                List.of(unannotated)))
                .withFailMessage(() -> "a tool without annotations was served")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("whatever");
    }

    private static SyncToolSpecification tool(String name, ToolAnnotations annotations) {
        return SyncToolSpecification.builder()
                .tool(Tool.builder(name, Map.of("type", "object")).annotations(annotations).build())
                .callHandler((_, _) -> CallToolResult.builder().build())
                .build();
    }
}
