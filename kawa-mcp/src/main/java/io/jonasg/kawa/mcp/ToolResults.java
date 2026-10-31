package io.jonasg.kawa.mcp;

import io.jonasg.kawa.http.configchange.ConflictException;
import io.jonasg.kawa.http.configchange.ForbiddenException;
import io.jonasg.kawa.http.configchange.NotFoundException;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/// Turns a tool's failure into a tool error (`isError: true`), so the agent sees it as the tool's
/// answer rather than a protocol error. A failure the caller can act on (not found, conflict,
/// refused, bad input) carries its message; any other failure is logged and answered with a
/// generic message, so internals do not leak to the agent.
final class ToolResults {

    static final String INTERNAL_ERROR = "internal error";

    private static final Logger log = LoggerFactory.getLogger(ToolResults.class);

    private ToolResults() {
    }

    /// Runs `call` and returns its result, or the tool error its failure maps to.
    static CallToolResult guarded(String toolName, Supplier<CallToolResult> call) {
        try {
            return call.get();
        } catch (NotFoundException | ConflictException | ForbiddenException | IllegalArgumentException e) {
            return error(e.getMessage());
        } catch (RuntimeException e) {
            log.warn("MCP tool {} failed", toolName, e);
            return error(INTERNAL_ERROR);
        }
    }

    private static CallToolResult error(String message) {
        return CallToolResult.builder().addTextContent(message).isError(true).build();
    }
}
