package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminAuthConfig;
import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.http.authentication.AdminAuthenticationContext;
import io.jonasg.kawa.http.authentication.AuthenticationProviderRegistry;
import io.jonasg.kawa.http.authentication.MapProviderSettings;
import io.jonasg.kawa.http.clients.ClientRoutes;
import io.jonasg.kawa.http.governance.GovernanceRoutes;
import io.jonasg.kawa.http.modules.ModuleRoutes;
import io.jonasg.kawa.http.oauthclients.OAuthClientRoutes;
import io.jonasg.kawa.http.rbac.RbacRoutes;
import io.jonasg.kawa.http.topics.TopicRoutes;
import io.jonasg.kawa.http.users.UserRoutes;
import io.jonasg.kawa.http.web.CorsConfigFactory;
import io.jonasg.kawa.http.web.GetDocsHandler;
import io.jonasg.kawa.http.web.HttpRouterHandler;
import io.jonasg.kawa.http.web.Router;
import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
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
/// [VirtualTopicManager][io.jonasg.kawa.virtualtopic.VirtualTopicManager] and
/// [MetadataCache][io.jonasg.kawa.core.cluster.MetadataCache], `POST /topics` and `DELETE /topics/{name}`
/// create/delete physical topics on the broker through the [TopicAdmin][io.jonasg.kawa.http.topics.TopicAdmin], and the
/// `/rbac/...`, `/auth/...`, `/governance` and `/modules` endpoints read and write the dynamic config
/// through the [DynamicConfigRepository][io.jonasg.kawa.config.DynamicConfigRepository]. Config writes
/// go to the config topic and are applied by the consumer.
///
/// Requests are authenticated by the provider that `admin.auth.type` selects from the
/// [AuthenticationProviderRegistry]: kawa's built-in `none` or `oauth`, or one registered by a
/// plugin. A provider may add public endpoints of its own (e.g. a token endpoint); every other
/// endpoint except `/docs` requires the provider to accept the request.
///
/// The routes dispatch to the [AdminServices] the server is given. Building those services seeds
/// `admin.auth.bootstrapUser` into the admin users (see [AdminServices#create]); the server creates
/// the authentication provider only afterwards, as a provider may require an admin user to exist.
public final class AdminHttpServer {

    private static final Logger log = LoggerFactory.getLogger(AdminHttpServer.class);

    private final AdminConfig config;
    private final Router router;
    private final AuthenticationProvider authenticationProvider;
    private final int routerExecutorThreads;

    private @Nullable EventLoopGroup bossGroup;
    private @Nullable EventLoopGroup workerGroup;
    private @Nullable EventExecutorGroup routerExecutorGroup;
    private @Nullable Channel serverChannel;

    /// @param services built by [AdminServices#create], which seeds the bootstrap user before the
    ///                 authentication provider is created here. The server does not close them.
    public AdminHttpServer(
            AdminConfig config,
            AdminServices services,
            AuthenticationProviderRegistry authenticationProviders
    ) {
        this.config = config;
        this.routerExecutorThreads = Math.max(4, Runtime.getRuntime().availableProcessors());
        AdminAuthConfig adminAuthConfig = config.auth();
        this.router = new Router();
        TopicRoutes.register(router, services.topics());
        RbacRoutes.register(router, services.roles(), services.groups());
        ClientRoutes.register(router, services.clients());
        GovernanceRoutes.register(router, services.governanceRules());
        UserRoutes.register(router, services.adminUsers());
        ModuleRoutes.register(router, services.modules());
        OAuthClientRoutes.register(router, services.oauthClients());
        router.publicGet("/docs", new GetDocsHandler());
        this.authenticationProvider = authenticationProviders.require(adminAuthConfig.type()).create(
                new AdminAuthenticationContext(new MapProviderSettings(adminAuthConfig.settings()),
                        services.adminUserAuthenticator(), config.resolvedPublicUrl(), router));
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
                        ch.pipeline().addLast(routerExecutorGroup, "httpRouter", new HttpRouterHandler(router, authenticationProvider));
                    }
                });
        serverChannel = bootstrap.bind(config.host(), config.port()).sync().channel();
        int boundPort = ((InetSocketAddress) serverChannel.localAddress()).getPort();
        log.info("Admin HTTP server listening on {}:{} (authentication: {})",
                config.host(), boundPort, config.auth().type());
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
    }

    /// The route table this server dispatches to. Other surfaces (e.g. MCP) may register routes on
    /// it, but only before [#start()].
    public Router router() {
        return router;
    }

    boolean hasDedicatedRouterExecutor() {
        return routerExecutorGroup != null;
    }

    @SuppressWarnings("ConstantConditions")
    public int boundPort() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }
}
