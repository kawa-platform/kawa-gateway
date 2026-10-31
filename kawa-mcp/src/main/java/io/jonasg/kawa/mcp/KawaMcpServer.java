package io.jonasg.kawa.mcp;

import io.jonasg.kawa.config.CorsConfig;
import io.jonasg.kawa.http.AdminServices;
import io.jonasg.kawa.http.web.Router;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpStatelessServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.server.McpStatelessSyncServer;
import io.modelcontextprotocol.server.transport.DefaultServerTransportSecurityValidator;
import io.modelcontextprotocol.server.transport.ServerTransportSecurityValidator;
import io.modelcontextprotocol.spec.McpSchema;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;

/// The gateway's MCP server, served at `POST /mcp` on the admin HTTP port. It registers its route on
/// the admin [Router], so it must be created before the admin server starts; `/mcp` then requires the
/// same authentication as every other admin endpoint.
///
/// Tools call the [AdminServices] directly and answer with views shaped for an agent (see
/// [ListTopicsTool]); a failure becomes a tool error (see [ToolResults]).
public final class KawaMcpServer implements AutoCloseable {

    static final String PATH = "/mcp";

    private final McpStatelessSyncServer server;

    /// @param router   the admin route table to serve `/mcp` from
    /// @param services the admin services the tools read from
    /// @param cors     the admin CORS config: browser origins it allows may call `/mcp`; without it,
    ///                 requests carrying an `Origin` header are refused
    public KawaMcpServer(Router router, AdminServices services, @Nullable CorsConfig cors) {
        var mapper = McpJsonDefaults.getMapper();
        this(router, cors, mapper, List.of(new ListTopicsTool(services.topics(), mapper).specification()));
    }

    /// Serves `tools`, which write their JSON with `mapper`.
    KawaMcpServer(Router router, @Nullable CorsConfig cors, McpJsonMapper mapper, List<SyncToolSpecification> tools) {
        var endpoint = new McpEndpoint(mapper, originValidator(cors));
        this.server = McpServer.sync(endpoint)
                .serverInfo("kawa", version())
                .jsonMapper(mapper)
                .capabilities(McpSchema.ServerCapabilities.builder().tools(false).build())
                .tools(tools)
                .build();
        router.post(PATH, endpoint);
    }

    /// Guards against DNS rebinding: a request with an `Origin` header must come from an origin the
    /// admin CORS config allows. Non-browser clients send no `Origin` and always pass.
    private static ServerTransportSecurityValidator originValidator(@Nullable CorsConfig cors) {
        if (cors == null) {
            return DefaultServerTransportSecurityValidator.builder().build();
        }
        if (cors.allowedOrigins().contains("*")) {
            return ServerTransportSecurityValidator.NOOP;
        }
        return DefaultServerTransportSecurityValidator.builder().allowedOrigins(cors.allowedOrigins()).build();
    }

    private static String version() {
        return Objects.requireNonNullElse(KawaMcpServer.class.getPackage().getImplementationVersion(), "dev");
    }

    @Override
    public void close() {
        server.closeGracefully();
    }
}
