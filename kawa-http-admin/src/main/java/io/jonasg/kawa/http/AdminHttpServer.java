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
import io.jonasg.kawa.http.web.GetDocsHandler;
import io.jonasg.kawa.http.web.NettyHttpServer;
import io.jonasg.kawa.http.web.Router;
import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Netty HTTP server exposing the gateway's admin/UI surface: `GET /topics` reads the
/// [VirtualTopicManager][io.jonasg.kawa.virtualtopic.VirtualTopicManager] and
/// [MetadataCache][io.jonasg.kawa.core.cluster.MetadataCache], `POST /topics` and `DELETE /topics/{name}`
/// create/delete physical topics on the broker through the [BrokerAdmin][io.jonasg.kawa.broker.BrokerAdmin] (a created
/// topic is waited for in the metadata cache, so the next `GET /topics` lists it), and the
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
/// `admin.auth.bootstrapUser` into the admin users and decides on setup mode (see [AdminServices#create]);
/// the server creates the authentication provider only afterwards. The public `/admin/setup` routes let
/// kawa studio create the first admin user of a gateway in setup mode.
public final class AdminHttpServer {

    private static final Logger log = LoggerFactory.getLogger(AdminHttpServer.class);

    private final AdminConfig config;
    private final Router router;
    private final NettyHttpServer http;

    /// @param services built by [AdminServices#create], which seeds the bootstrap user before the
    ///                 authentication provider is created here. The server does not close them.
    public AdminHttpServer(
            AdminConfig config,
            AdminServices services,
            AuthenticationProviderRegistry authenticationProviders
    ) {
        this.config = config;
        AdminAuthConfig adminAuthConfig = config.auth();
        this.router = new Router();
        TopicRoutes.register(router, services.topics());
        RbacRoutes.register(router, services.roles(), services.groups());
        ClientRoutes.register(router, services.clients());
        GovernanceRoutes.register(router, services.governanceRules());
        UserRoutes.register(router, services.adminUsers(), services.setup());
        ModuleRoutes.register(router, services.modules());
        OAuthClientRoutes.register(router, services.oauthClients());
        router.publicGet("/docs", new GetDocsHandler());
        AuthenticationProvider authenticationProvider = authenticationProviders.require(adminAuthConfig.type()).create(
                new AdminAuthenticationContext(new MapProviderSettings(adminAuthConfig.settings()),
                        services.adminUserAuthenticator(), services.oauthClientDirectory(),
                        services.modules()::clientIdMetadataDocumentsEnabled, config.resolvedPublicUrl(), router));
        this.http = new NettyHttpServer(config.host(), config.port(), router, config.cors(), authenticationProvider);
    }

    public void start() throws InterruptedException {
        http.start();
        log.info("Admin HTTP server listening on {}:{} (authentication: {})",
                config.host(), http.boundPort(), config.auth().type());
    }

    public void stop() {
        http.stop();
    }

    /// The route table this server dispatches to. Other surfaces (e.g. MCP) may register routes on
    /// it, but only before [#start()].
    public Router router() {
        return router;
    }

    boolean hasDedicatedRouterExecutor() {
        return http.hasDedicatedRouterExecutor();
    }

    public int boundPort() {
        return http.boundPort();
    }
}
