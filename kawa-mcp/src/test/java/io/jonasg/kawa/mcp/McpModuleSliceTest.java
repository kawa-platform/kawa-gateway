package io.jonasg.kawa.mcp;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.awaitility.Awaitility.await;

/// Slice tests for switching `/mcp` with the `mcp` module through `PUT /modules/mcp` on a booted
/// admin server.
class McpModuleSliceTest extends McpSliceTestBase {

    @Test
    void answersNotFoundOnceTheMcpModuleIsDisabled() throws Exception {
        // given
        startServer();

        // when
        var disableMcpResp = send("PUT", "/modules/mcp", """
                {"enabled": false}""");

        // then
        assertThat(disableMcpResp).hasStatusCode(200);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var toolsResp = rpc("tools/list", "{}");
            assertThat(toolsResp).hasStatusCode(404);
            assertThatJson(toolsResp.body()).isEqualTo("""
                    {"error": "not found"}""");
        });
    }

    @Test
    void servesMcpAgainOnceTheMcpModuleIsReEnabled() throws Exception {
        // given
        startServer();
        send("PUT", "/modules/mcp", """
                {"enabled": false}""");
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(rpc("tools/list", "{}")).hasStatusCode(404));

        // when
        var enableMcpResp = send("PUT", "/modules/mcp", """
                {"enabled": true}""");

        // then
        assertThat(enableMcpResp).hasStatusCode(200);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var toolsResp = rpc("tools/list", "{}");
            assertThat(toolsResp).hasStatusCode(200);
            assertThatJson(toolsResp.body()).inPath("result.tools[*].name").isArray().containsExactly("list_topics", "describe_topic");
        });
    }
}
