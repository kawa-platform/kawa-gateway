package io.jonasg.kawa.mcp;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;

/// Unit tests for the per-call log line of a guarded [McpTool] call, with a caller that came through an
/// OAuth client (the slice tests run with the `none` provider, whose caller has none).
class McpToolTest {

    private static final McpTransportContext ALICE_VIA_CLAUDE = McpTransportContext.create(
            Map.of(McpEndpoint.PRINCIPAL, new KawaPrincipal("alice", "kawa-mcp-claude")));

    private final Logger logger = (Logger) LoggerFactory.getLogger(McpTool.class);
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
        call(ALICE_VIA_CLAUDE, Map::of);

        // then
        assertThat(infoLines())
                .withFailMessage(() -> "the successful call was not logged once with caller and client: " + infoLines())
                .containsExactly("MCP tool list_topics called by alice via kawa-mcp-claude: ok");
    }

    @Test
    void logsAFailureTheCallerCanActOnAsError() {
        // when
        call(ALICE_VIA_CLAUDE, () -> {
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
        call(ALICE_VIA_CLAUDE, () -> {
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
        call(context, Map::of);

        // then
        assertThat(infoLines())
                .withFailMessage(() -> "the call without a client was not logged with client '-': " + infoLines())
                .containsExactly("MCP tool list_topics called by admin via -: ok");
    }

    @Test
    void logsADashForAMissingCaller() {
        // when
        call(McpTransportContext.EMPTY, Map::of);

        // then
        assertThat(infoLines())
                .withFailMessage(() -> "the call without a principal was not logged with user and client '-': " + infoLines())
                .containsExactly("MCP tool list_topics called by - via -: ok");
    }

    /// Calls `list_topics` for the caller in `context` through the registered handler, with `work` as the tool's work.
    private static CallToolResult call(McpTransportContext context, Supplier<Object> work) {
        var tool = new McpTool(McpJsonDefaults.getMapper()) {
            @Override
            protected String name() {
                return "list_topics";
            }

            @Override
            protected String description() {
                return "stub";
            }

            @Override
            protected String inputSchema() {
                return "{\"type\":\"object\"}";
            }

            @Override
            protected String outputSchema() {
                return "{\"type\":\"object\"}";
            }

            @Override
            protected Object call(CallToolRequest request) {
                return work.get();
            }
        };
        return tool.specification().callHandler().apply(context, new CallToolRequest("list_topics", Map.of()));
    }

    private List<String> infoLines() {
        return logs.list.stream()
                .filter(event -> event.getLevel() == Level.INFO)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }
}
