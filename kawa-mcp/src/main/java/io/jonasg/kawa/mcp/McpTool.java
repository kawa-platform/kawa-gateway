package io.jonasg.kawa.mcp;

import io.jonasg.kawa.http.configchange.ConflictException;
import io.jonasg.kawa.http.configchange.ForbiddenException;
import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpStatelessServerFeatures;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

/// An MCP tool. The template method [#specification()] registers the tool and guards every call, so a
/// failure becomes a tool error (`isError: true`) that the agent sees as the tool's answer rather than a
/// protocol error, and each call is logged once. A subclass supplies its metadata through [#name()],
/// [#description()], [#inputSchema()] and [#outputSchema()], and its work through
/// [#call(CallToolRequest)], which returns the view the tool answers with. The base serialises that view
/// as both structured content and JSON text.
///
/// `call` throws on bad input or a missing resource; it does not build error results itself. A failure
/// the caller can act on (not found, conflict, refused, bad input) carries its message; any other failure
/// is logged and answered with [#INTERNAL_ERROR], so internals do not leak to the agent.
///
/// Every call leaves one info line naming the tool, the caller and the OAuth client it came through,
/// e.g. `MCP tool list_topics called by alice via kawa-mcp-claude: ok`, so agent activity can be traced.
/// Arguments and results are never logged: a tool's input may be sensitive.
public abstract class McpTool {

    public static final String INTERNAL_ERROR = "internal error";

    private static final String UNKNOWN = "-";
    private static final Logger log = LoggerFactory.getLogger(McpTool.class);

    private final McpJsonMapper mapper;

    /// @param mapper writes the tool's JSON; the server passes the mapper it serves with
    protected McpTool(McpJsonMapper mapper) {
        this.mapper = mapper;
    }

    /// The tool name the agent calls.
    protected abstract String name();

    /// What the tool does, as the agent reads it.
    protected abstract String description();

    /// The JSON Schema of the arguments.
    protected abstract String inputSchema();

    /// The JSON Schema of the view [#call(CallToolRequest)] returns.
    protected abstract String outputSchema();

    /// Does the tool's work and returns the view it answers with. Throws on bad input or a missing resource.
    protected abstract Object call(CallToolRequest request);

    /// The tool's registration, with every call guarded and logged. Subclasses do not override it.
    public final McpStatelessServerFeatures.SyncToolSpecification specification() {
        var tool = Tool.builder(name(), mapper, inputSchema())
                .description(description())
                .outputSchema(mapper, outputSchema())
                .build();
        return McpStatelessServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler(this::guarded)
                .build();
    }

    /// Runs the call for the caller in `context` and returns its result, or the tool error its failure maps to.
    private CallToolResult guarded(McpTransportContext context, CallToolRequest request) {
        var toolName = name();
        CallToolResult result;
        String outcome;
        try {
            result = result(request);
            outcome = "ok";
        } catch (NotFoundException | ConflictException | ForbiddenException | IllegalArgumentException e) {
            result = error(e.getMessage());
            outcome = "error";
        } catch (McpToolException e) {
            log.warn("MCP tool exception", e);
            result = error(e.getMessage());
            outcome = "failed";
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

    private CallToolResult result(CallToolRequest request) {
        var view = call(request);
        try {
            return CallToolResult.builder()
                    .structuredContent(view)
                    .addTextContent(mapper.writeValueAsString(view))
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static CallToolResult error(String message) {
        return CallToolResult.builder().addTextContent(message).isError(true).build();
    }

    /// The string argument `name`, which the call must carry.
    ///
    /// @throws IllegalArgumentException when it is absent, blank or not a string
    protected final String getRequiredArg(CallToolRequest request, String name) {
        return getOptionalArg(request, name)
                .orElseThrow(() -> new IllegalArgumentException(name + " is required"));
    }

    /// The string argument `name`, or empty when it is absent or blank.
    ///
    /// @throws IllegalArgumentException when it is present but not a string
    protected final Optional<String> getOptionalArg(CallToolRequest request, String name) {
        var arguments = request.arguments();
        var argument = arguments == null ? null : arguments.get(name);
        return switch (argument) {
            case null -> Optional.empty();
            case String value when value.isBlank() -> Optional.empty();
            case String value -> Optional.of(value);
            default -> throw new IllegalArgumentException(name + " must be a string");
        };
    }
}
