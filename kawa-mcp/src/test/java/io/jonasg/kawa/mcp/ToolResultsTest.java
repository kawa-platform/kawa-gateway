package io.jonasg.kawa.mcp;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;

/// Unit tests for the per-call log line of [ToolResults#guarded], with a caller that came through an
/// OAuth client (the slice tests run with the `none` provider, whose caller has none).
class ToolResultsTest {

    private static final McpTransportContext ALICE_VIA_CLAUDE = McpTransportContext.create(
            Map.of(McpEndpoint.PRINCIPAL, new KawaPrincipal("alice", "kawa-mcp-claude")));

    private final Logger logger = (Logger) LoggerFactory.getLogger(ToolResults.class);
    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();

    @BeforeEach
    void captureLogs() {
        logs.start();
        logger.addAppender(logs);
    }

    @AfterEach
    void releaseLogs() {
        logger.detachAppender(logs);
    }

    @Test
    void logsTheCallerClientAndToolOfASuccessfulCall() {
        // when
        ToolResults.guarded("list_topics", ALICE_VIA_CLAUDE, () -> CallToolResult.builder().addTextContent("{}").build());

        // then
        assertThat(infoLines())
                .withFailMessage(() -> "the successful call was not logged once with caller and client: " + infoLines())
                .containsExactly("MCP tool list_topics called by alice via kawa-mcp-claude: ok");
    }

    @Test
    void logsAToolErrorResultAsError() {
        // when
        ToolResults.guarded("list_topics", ALICE_VIA_CLAUDE,
                () -> CallToolResult.builder().addTextContent("nope").isError(true).build());

        // then
        assertThat(infoLines())
                .withFailMessage(() -> "the tool error was not logged once as error: " + infoLines())
                .containsExactly("MCP tool list_topics called by alice via kawa-mcp-claude: error");
    }

    @Test
    void logsAFailureTheCallerCanActOnAsError() {
        // when
        ToolResults.guarded("list_topics", ALICE_VIA_CLAUDE, () -> {
            throw new NotFoundException("topic 'orders' not found");
        });

        // then
        assertThat(infoLines())
                .withFailMessage(() -> "the not-found failure was not logged once as error: " + infoLines())
                .containsExactly("MCP tool list_topics called by alice via kawa-mcp-claude: error");
    }

    @Test
    void logsAnUnexpectedFailureAsFailed() {
        // when
        ToolResults.guarded("list_topics", ALICE_VIA_CLAUDE, () -> {
            throw new IllegalStateException("metadata cache exploded");
        });

        // then
        assertThat(infoLines())
                .withFailMessage(() -> "the unexpected failure was not logged once as failed: " + infoLines())
                .containsExactly("MCP tool list_topics called by alice via kawa-mcp-claude: failed");
    }

    @Test
    void logsADashForACallerWithoutAClient() {
        // given
        var context = McpTransportContext.create(Map.of(McpEndpoint.PRINCIPAL, new KawaPrincipal("admin")));

        // when
        ToolResults.guarded("list_topics", context, () -> CallToolResult.builder().addTextContent("{}").build());

        // then
        assertThat(infoLines())
                .withFailMessage(() -> "the call without a client was not logged with client '-': " + infoLines())
                .containsExactly("MCP tool list_topics called by admin via -: ok");
    }

    @Test
    void logsADashForAMissingCaller() {
        // when
        ToolResults.guarded("list_topics", McpTransportContext.EMPTY,
                () -> CallToolResult.builder().addTextContent("{}").build());

        // then
        assertThat(infoLines())
                .withFailMessage(() -> "the call without a principal was not logged with user and client '-': " + infoLines())
                .containsExactly("MCP tool list_topics called by - via -: ok");
    }

    private List<String> infoLines() {
        return logs.list.stream()
                .filter(event -> event.getLevel() == Level.INFO)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }
}
