package io.jonasg.kawa.mcp;

import io.jonasg.kawa.config.CorsConfig;
import io.jonasg.kawa.core.module.ModuleRegistry;
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
/// same authentication as every other admin endpoint. While the
/// [ModuleId#MCP][io.jonasg.kawa.config.ModuleId#MCP] module is switched off, `/mcp` answers `404`.
///
/// Tools call the [AdminServices] directly and answer with views shaped for an agent (see
/// [ListTopicsTool], [ListConsumerGroupsTool], [DescribeConsumerGroupTool]); a failure becomes a tool error (see
/// [ToolResults]). Every tool only reads: one that is not annotated read-only is refused at startup.
public final class KawaMcpServer implements AutoCloseable {

    static final String PATH = "/mcp";

    private final McpStatelessSyncServer server;

    /// @param router   the admin route table to serve `/mcp` from
    /// @param services the admin services the tools read from
    /// @param modules  the gateway's modules; `/mcp` is served only while
    ///                 [ModuleId#MCP][io.jonasg.kawa.config.ModuleId#MCP] is enabled
    /// @param cors     the admin CORS config: browser origins it allows may call `/mcp`; without it,
    ///                 requests carrying an `Origin` header are refused
    public KawaMcpServer(Router router, AdminServices services, ModuleRegistry modules, @Nullable CorsConfig cors) {
        var mapper = McpJsonDefaults.getMapper();
        this(router, modules, cors, mapper, List.of(
                new ListTopicsTool(services.topics(), mapper).specification(),
                new ListConsumerGroupsTool(services.consumerGroups(), services.topics(), mapper).specification(),
                new DescribeConsumerGroupTool(services.consumerGroups(), services.topics(), mapper).specification()));
    }

    /// Serves `tools`, which write their JSON with `mapper`.
    ///
    /// @throws IllegalArgumentException when a tool is not annotated read-only (see [ToolResults#READ_ONLY]):
    ///                                  the MCP server only serves tools that read
    KawaMcpServer(
            Router router,
            ModuleRegistry modules,
            @Nullable CorsConfig cors,
            McpJsonMapper mapper,
            List<SyncToolSpecification> tools
    ) {
        requireReadOnly(tools);
        var endpoint = new McpEndpoint(modules, mapper, originValidator(cors));
        this.server = McpServer.sync(endpoint)
                .serverInfo("kawa", version())
                .jsonMapper(mapper)
                .capabilities(McpSchema.ServerCapabilities.builder().tools(false).build())
                .tools(tools)
                .build();
        router.post(PATH, endpoint);
    }

    private static void requireReadOnly(List<SyncToolSpecification> tools) {
        tools.stream()
                .map(SyncToolSpecification::tool)
                .filter(tool -> !isReadOnly(tool.annotations()))
                .findFirst()
                .ifPresent(tool -> {
                    throw new IllegalArgumentException("MCP tool '" + tool.name()
                            + "' is not read-only: kawa's MCP server only serves tools that read");
                });
    }

    private static boolean isReadOnly(McpSchema.@Nullable ToolAnnotations annotations) {
        return annotations != null
                && Boolean.TRUE.equals(annotations.readOnlyHint())
                && Boolean.FALSE.equals(annotations.destructiveHint());
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
