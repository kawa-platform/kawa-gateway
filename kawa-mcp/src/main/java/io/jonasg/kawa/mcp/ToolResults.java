package io.jonasg.kawa.mcp;

import io.jonasg.kawa.http.configchange.ConflictException;
import io.jonasg.kawa.http.configchange.ForbiddenException;
import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/// Turns a tool's failure into a tool error (`isError: true`), so the agent sees it as the tool's
/// answer rather than a protocol error. A failure the caller can act on (not found, conflict,
/// refused, bad input) carries its message; any other failure is logged and answered with a
/// generic message, so internals do not leak to the agent.
///
/// Every call leaves one info line naming the tool, the caller and the OAuth client it came
/// through, e.g. `MCP tool list_topics called by alice via kawa-mcp-claude: ok`, so agent activity
/// can be traced. Arguments and results are never logged: a tool's input may be sensitive.
final class ToolResults {

    static final String INTERNAL_ERROR = "internal error";

    private static final String UNKNOWN = "-";
    private static final Logger log = LoggerFactory.getLogger(ToolResults.class);

    private ToolResults() {
    }

    /// Runs `call` for the caller in `context` and returns its result, or the tool error its failure maps to.
    static CallToolResult guarded(String toolName, McpTransportContext context, Supplier<CallToolResult> call) {
        CallToolResult result;
        String outcome;
        try {
            result = call.get();
            outcome = Boolean.TRUE.equals(result.isError()) ? "error" : "ok";
        } catch (NotFoundException | ConflictException | ForbiddenException | IllegalArgumentException e) {
            result = error(e.getMessage());
            outcome = "error";
        } catch (RuntimeException e) {
            log.warn("MCP tool {} failed", toolName, e);
            result = error(INTERNAL_ERROR);
            outcome = "failed";
        }
        var principal = context.get(McpEndpoint.PRINCIPAL) instanceof KawaPrincipal p ? p : null;
        log.info("MCP tool {} called by {} via {}: {}", toolName,
                principal == null ? UNKNOWN : principal.name(),
                principal == null || principal.clientId() == null ? UNKNOWN : principal.clientId(),
                outcome);
        return result;
    }

    private static CallToolResult error(String message) {
        return CallToolResult.builder().addTextContent(message).isError(true).build();
    }
}
