package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/// Netty HTTP server exposing the gateway's admin/UI surface: `GET /topics` reads the
/// [VirtualTopicManager] and [MetadataCache], `POST /topics` and `DELETE /topics/{name}`
/// create/delete physical topics on the broker through the [TopicAdmin], and the
/// `/rbac/...`, `/auth/...` and `/governance` endpoints read and write the dynamic config
/// through the [GatewayConfigRepository]. Config writes go to the config topic and are
/// applied by the consumer.
public final class AdminHttpServer {

    private static final Logger log = LoggerFactory.getLogger(AdminHttpServer.class);

    private final AdminConfig config;
    private final TopicAdmin topicAdmin;
    private final Router router;
    private final int routerExecutorThreads;

    private @Nullable EventLoopGroup bossGroup;
    private @Nullable EventLoopGroup workerGroup;
    private @Nullable EventExecutorGroup routerExecutorGroup;
    private @Nullable Channel serverChannel;

    public AdminHttpServer(
            AdminConfig config,
            VirtualTopicManager virtualTopics,
            MetadataCache cache,
            GatewayConfigRepository configRepository,
            GovernancePolicy governance,
            TopicAdmin topicAdmin
    ) {
        this.config = config;
        this.topicAdmin = topicAdmin;
        this.routerExecutorThreads = Math.max(4, Runtime.getRuntime().availableProcessors());
        var topicService = new TopicService(virtualTopics, cache, configRepository, topicAdmin, governance);
        var roleService = new RbacRoleService(configRepository);
        var groupService = new RbacGroupService(configRepository);
        var clientService = new AuthClientService(configRepository);
        var governanceService = new GovernanceService(configRepository);
        var governanceMapper = new GovernanceConfigMapper();
        this.router = new Router()
                .get("/topics", new GetTopicsHandler(topicService))
                .post("/topics", new PostTopicHandler(topicService))
                .put("/topics/{name}", new PutTopicHandler(topicService))
                .patch("/topics/{name}", new PatchTopicHandler(topicService))
                .delete("/topics/{name}", new DeleteTopicHandler(topicService))

                .get("/rbac/roles", new GetRbacRolesHandler(roleService))
                .put("/rbac/roles/{name}", new PutRbacRoleHandler(roleService))
                .delete("/rbac/roles/{name}", new DeleteRbacRoleHandler(roleService))
                .get("/rbac/groups", new GetRbacGroupsHandler(groupService))
                .put("/rbac/groups/{name}", new PutRbacGroupHandler(groupService))
                .delete("/rbac/groups/{name}", new DeleteRbacGroupHandler(groupService))
                .patch("/rbac/groups/{name}", new PatchRbacGroupHandler(groupService))

                .get("/auth/clients", new GetAuthClientsHandler(clientService))
                .put("/auth/clients/{name}", new PutAuthClientHandler(clientService))
                .patch("/auth/clients/{name}", new PatchAuthClientHandler(clientService))
                .delete("/auth/clients/{name}", new DeleteAuthClientHandler(clientService))

                .get("/governance/rules", new GetGovernanceRulesHandler(governanceService, governanceMapper))
                .get("/governance/rules/{name}", new GetGovernanceRuleHandler(governanceService, governanceMapper))
                .put("/governance/rules/{name}", new PutGovernanceRuleHandler(governanceService, governanceMapper))
                .delete("/governance/rules/{name}", new DeleteGovernanceRuleHandler(governanceService))
                .get("/governance/exemptions", new GetGovernanceExemptionsHandler(governanceService, governanceMapper))
                .get("/governance/exemptions/{name}", new GetGovernanceExemptionHandler(governanceService, governanceMapper))
                .put("/governance/exemptions/{name}", new PutGovernanceExemptionHandler(governanceService, governanceMapper))
                .delete("/governance/exemptions/{name}", new DeleteGovernanceExemptionHandler(governanceService))
                .post("/governance/dry-run", new PostGovernanceDryRunHandler(governance, governanceService, governanceMapper))
                .get("/governance/variables", new GetGovernanceVariablesHandler(governanceService, governanceMapper))
                .put("/governance/variables/{name}", new PutGovernanceVariableHandler(governanceService, governanceMapper))
                .delete("/governance/variables/{name}", new DeleteGovernanceVariableHandler(governanceService))

                .get("/docs", new GetDocsHandler());
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        routerExecutorGroup = new DefaultEventExecutorGroup(routerExecutorThreads);
        var bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast("httpCodec", new HttpServerCodec());
                        ch.pipeline().addLast("httpAggregator", new HttpObjectAggregator(65536));
                        if (config.cors() != null) {
                            ch.pipeline().addLast("cors", new CorsHandler(CorsConfigFactory.from(config.cors())));
                        }
                        ch.pipeline().addLast(routerExecutorGroup, "httpRouter", new HttpRouterHandler(router));
                    }
                });
        serverChannel = bootstrap.bind(config.host(), config.port()).sync().channel();
        int boundPort = ((InetSocketAddress) serverChannel.localAddress()).getPort();
        log.info("Admin HTTP server listening on {}:{}", config.host(), boundPort);
    }

    @SuppressWarnings("ConstantConditions")
    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
        if (routerExecutorGroup != null) {
            routerExecutorGroup.shutdownGracefully();
        }
        topicAdmin.close();
    }

    boolean hasDedicatedRouterExecutor() {
        return routerExecutorGroup != null;
    }

    @SuppressWarnings("ConstantConditions")
    public int boundPort() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }
}
