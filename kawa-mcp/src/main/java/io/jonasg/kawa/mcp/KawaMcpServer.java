package io.jonasg.kawa.mcp;

import io.jonasg.kawa.broker.RecordReader;
import io.jonasg.kawa.config.CorsConfig;
import io.jonasg.kawa.http.AdminServices;
import io.jonasg.kawa.http.modules.ModuleService;
import io.jonasg.kawa.http.web.Router;
import io.jonasg.kawa.mcp.describeconsumergroup.DescribeConsumerGroupMcpTool;
import io.jonasg.kawa.mcp.describeconsumergroup.ConsumerGroupMcpResource;
import io.jonasg.kawa.mcp.describetopic.DescribeTopicMcpTool;
import io.jonasg.kawa.mcp.describetopic.TopicMcpResource;
import io.jonasg.kawa.mcp.listconsumergroups.ConsumerGroupsMcpResource;
import io.jonasg.kawa.mcp.listconsumergroups.ListConsumerGroupsMcpTool;
import io.jonasg.kawa.mcp.listtopics.ListTopicsMcpTool;
import io.jonasg.kawa.mcp.listtopics.TopicsMcpResource;
import io.jonasg.kawa.mcp.peekrecords.PeekRecordsMcpTool;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistries;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
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
/// [ListTopicsMcpTool]); a failure becomes a tool error (see [McpTool]).
public final class KawaMcpServer implements AutoCloseable {

    static final String PATH = "/mcp";

    private final McpStatelessSyncServer server;

    /// @param router   the admin route table to serve `/mcp` from
    /// @param services the admin services the tools read from
    /// @param cors             the admin CORS config: browser origins it allows may call `/mcp`; without it,
    ///                         requests carrying an `Origin` header are refused
    /// @param recordReader     reads topic records for `peek_records`
    /// @param schemaRegistries the schema registry of each physical topic's cluster, to decode Avro values with
    public KawaMcpServer(Router router, AdminServices services, @Nullable CorsConfig cors, RecordReader recordReader,
                         SchemaRegistries schemaRegistries) {
        var mapper = McpJsonDefaults.getMapper();
        this(
                router,
                services.modules(),
                cors,
                mapper,
                List.of(
                        new ListTopicsMcpTool(mapper, services.topics()),
                        new DescribeTopicMcpTool(mapper, services.topics()),
                        new ListConsumerGroupsMcpTool(mapper, services.consumerGroups()),
                        new DescribeConsumerGroupMcpTool(mapper, services.consumerGroups()),
                        new PeekRecordsMcpTool(mapper, services.topics(), recordReader, schemaRegistries)
                ),
                List.of(
                        new TopicsMcpResource(mapper, services.topics()),
                        new ConsumerGroupsMcpResource(mapper, services.consumerGroups())
                ),
                List.of(
                        new TopicMcpResource(mapper, services.topics()),
                        new ConsumerGroupMcpResource(mapper, services.consumerGroups())
                )
        );
    }

    /// Serves `tools`, the fixed-URI `resources` and the URI-template `resourceTemplates`, which write their JSON
    /// with `mapper`.
    KawaMcpServer(
            Router router,
            ModuleService modules,
            @Nullable CorsConfig cors,
            McpJsonMapper mapper,
            List<McpTool> tools,
            List<McpResource> resources,
            List<McpResource> resourceTemplates
    ) {
        var endpoint = new McpEndpoint(modules, mapper, originValidator(cors));
        this.server = McpServer.sync(endpoint)
                .serverInfo("kawa", version())
                .jsonMapper(mapper)
                .capabilities(McpSchema.ServerCapabilities.builder().tools(false).resources(false, false).completions().build())
                .tools(tools.stream().map(McpTool::specification).toList())
                .resources(resources.stream().map(McpResource::specification).toList())
                .resourceTemplates(resourceTemplates.stream().map(McpResource::templateSpecification).toList())
                .completions(resourceTemplates.stream().map(McpResource::completionSpecification).toList())
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
