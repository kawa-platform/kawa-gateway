package io.jonasg.kawa.mcp;

import io.jonasg.kawa.http.Router;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.server.McpStatelessServerFeatures;
import io.modelcontextprotocol.spec.McpSchema;
import io.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

/// An MCP tool served by an admin [Router] route: a call is dispatched to the route's handler as the
/// authenticated MCP caller, so the tool answers with exactly the JSON the REST endpoint returns and
/// reuses its handler, mapper and service. A status `>= 400` becomes a tool error.
final class RouteTool {

    private static final Logger log = LoggerFactory.getLogger(RouteTool.class);
    private static final Map<String, Object> NO_ARGUMENTS = Map.of("type", "object", "properties", Map.of());

    private final Router router;
    private final JsonMapper mapper = JsonMapper.builder().build();

    RouteTool(Router router) {
        this.router = router;
    }

    /// A tool without arguments that serves `method path`.
    McpStatelessServerFeatures.SyncToolSpecification noArguments(
            String name, String description, HttpMethod method, String path) {
        return McpStatelessServerFeatures.SyncToolSpecification.builder()
                .tool(McpSchema.Tool.builder(name, NO_ARGUMENTS).description(description).build())
                .callHandler((context, _) -> call(context, method, path))
                .build();
    }

    private McpSchema.CallToolResult call(McpTransportContext context, HttpMethod method, String path) {
        Router.Match match = router.find(method, path)
                .orElseThrow(() -> new IllegalStateException("no route for " + method + " " + path));
        var request = new Router.Request(method.name(), path, match.pathParams(), Map.of(), Map.of(), new byte[0],
                (KawaPrincipal) context.get(McpEndpoint.PRINCIPAL));
        try {
            Router.Response<?> response = match.handler().handle(request);
            String text = response.rawBody()
                    ? (String) response.body()
                    : response.body() == null ? "" : mapper.writeValueAsString(response.body());
            return McpSchema.CallToolResult.builder()
                    .addTextContent(text)
                    .isError(response.status() >= 400)
                    .build();
        } catch (Exception e) {
            log.warn("MCP tool for {} {} failed", method, path, e);
            return McpSchema.CallToolResult.builder()
                    .addTextContent("{\"error\":\"handler failed\"}")
                    .isError(true)
                    .build();
        }
    }
}
