package io.jonasg.kawa.mcp;

import io.jonasg.kawa.http.Router;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpStatelessServerHandler;
import io.modelcontextprotocol.server.transport.ServerTransportSecurityException;
import io.modelcontextprotocol.server.transport.ServerTransportSecurityValidator;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpStatelessServerTransport;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/// The MCP stateless Streamable HTTP transport as a [Router.Handler] for `POST /mcp`: one JSON-RPC
/// message per request, answered with one JSON response; no sessions and no SSE stream.
///
/// It is a Netty-free port of the SDK's `HttpServletStatelessServerTransport`, so `/mcp` reuses the
/// admin server's HTTP pipeline, authentication and router executor (which makes blocking on the
/// SDK's `Mono`s safe). The authenticated caller is put into the [McpTransportContext] under
/// [#PRINCIPAL] for the tools to act as.
final class McpEndpoint implements Router.Handler, McpStatelessServerTransport {

    /// [McpTransportContext] key of the authenticated caller's [io.jonasg.kawa.plugin.auth.KawaPrincipal].
    static final String PRINCIPAL = "kawa.principal";

    private static final Logger log = LoggerFactory.getLogger(McpEndpoint.class);
    private static final String JSON = "application/json";
    private static final String EVENT_STREAM = "text/event-stream";

    private final McpJsonMapper mapper;
    private final ServerTransportSecurityValidator securityValidator;
    private @Nullable McpStatelessServerHandler mcpHandler;
    private volatile boolean closing;

    McpEndpoint(McpJsonMapper mapper, ServerTransportSecurityValidator securityValidator) {
        this.mapper = mapper;
        this.securityValidator = securityValidator;
    }

    @Override
    public void setMcpHandler(McpStatelessServerHandler mcpHandler) {
        this.mcpHandler = mcpHandler;
    }

    @Override
    public Mono<Void> closeGracefully() {
        return Mono.fromRunnable(() -> closing = true);
    }

    @Override
    @SuppressWarnings("ConstantConditions") // mcpHandler is set by McpServer.sync(...).build()
    public Router.Response<?> handle(Router.Request request) {
        if (closing) {
            return error(503, McpSchema.ErrorCodes.INTERNAL_ERROR, "Server is shutting down");
        }
        try {
            securityValidator.validateHeaders(request.headers());
        } catch (ServerTransportSecurityException e) {
            return error(e.getStatusCode(), McpSchema.ErrorCodes.INVALID_REQUEST, e.getMessage());
        }
        if (!request.header("content-type").map(type -> type.contains(JSON)).orElse(false)) {
            return error(415, McpSchema.ErrorCodes.INVALID_REQUEST,
                    "Unsupported Media Type: Content-Type must be application/json");
        }
        String accept = request.header("accept").orElse("");
        if (!(accept.contains(JSON) && accept.contains(EVENT_STREAM))) {
            return error(400, McpSchema.ErrorCodes.INVALID_REQUEST,
                    "Both application/json and text/event-stream required in Accept header");
        }

        McpSchema.JSONRPCMessage message;
        try {
            message = McpSchema.deserializeJsonRpcMessage(mapper, new String(request.body(), StandardCharsets.UTF_8));
        } catch (IOException | IllegalArgumentException e) {
            return error(400, McpSchema.ErrorCodes.INVALID_REQUEST, "Invalid message format");
        }

        var context = McpTransportContext.create(
                request.principal() == null ? Map.of() : Map.of(PRINCIPAL, request.principal()));
        try {
            return switch (message) {
                case McpSchema.JSONRPCRequest rpcRequest -> {
                    McpSchema.JSONRPCResponse response = mcpHandler.handleRequest(context, rpcRequest)
                            .contextWrite(ctx -> ctx.put(McpTransportContext.KEY, context))
                            .block();
                    yield Router.Response.raw(200, mapper.writeValueAsString(response), JSON);
                }
                case McpSchema.JSONRPCNotification notification -> {
                    mcpHandler.handleNotification(context, notification)
                            .contextWrite(ctx -> ctx.put(McpTransportContext.KEY, context))
                            .block();
                    yield Router.Response.raw(202, "", JSON);
                }
                default -> error(400, McpSchema.ErrorCodes.INVALID_REQUEST,
                        "The server accepts either requests or notifications");
            };
        } catch (Exception e) {
            log.error("Failed to handle MCP message", e);
            return error(500, McpSchema.ErrorCodes.INTERNAL_ERROR, "Failed to handle request");
        }
    }

    /// A JSON-RPC error response with a `null` id, for messages that could not be dispatched.
    private Router.Response<?> error(int status, int code, String message) {
        var body = new LinkedHashMap<String, @Nullable Object>();
        body.put("jsonrpc", McpSchema.JSONRPC_VERSION);
        body.put("id", null);
        body.put("error", new McpSchema.JSONRPCResponse.JSONRPCError(code, message));
        try {
            return Router.Response.raw(status, mapper.writeValueAsString(body), JSON);
        } catch (IOException e) {
            return Router.Response.internalError("serialization failed");
        }
    }
}
