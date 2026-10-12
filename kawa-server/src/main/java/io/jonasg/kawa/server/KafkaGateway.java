package io.jonasg.kawa.server;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.BrokerSecurity;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.ModuleId;
import io.jonasg.kawa.core.Gateway;
import io.jonasg.kawa.http.auth.AuthenticationProviderRegistry;
import io.jonasg.kawa.server.plugin.PluginLoader;
import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.ApiVersionsResponseBuilder;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.SupportedVersions;
import io.jonasg.kawa.governance.GovernanceInterceptor;
import io.jonasg.kawa.governance.TopicDescriber;
import io.jonasg.kawa.http.KafkaTopicAdmin;
import io.jonasg.kawa.rbac.AuthorizationInterceptor;
import io.jonasg.kawa.server.broker.BrokerConnectionSettings;
import io.jonasg.kawa.server.broker.ClusterConnections;
import io.jonasg.kawa.server.endpoint.BrokerEndpointInterceptor;
import io.jonasg.kawa.server.netty.KafkaListener;
import io.jonasg.kawa.server.netty.ListenerTls;
import io.jonasg.kawa.virtualtopic.FetchSessionRegistry;
import io.jonasg.kawa.virtualtopic.VirtualTopicInterceptor;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Set;

/// Netty-based Kafka gateway: accepts client connections, rewrites virtual topics and
/// forwards traffic to the physical cluster.
///
/// The constructor takes the static bootstrap config: cluster bootstrap servers, broker
/// credentials, config topic name, listeners, advertised endpoint and admin. Virtual topics,
/// RBAC and client auth are read from the config topic and keep updating live via
/// [DynamicConfigManager]; clusters, listeners, admin and advertised stay startup-only.
/// An empty config topic on first boot is valid - the gateway boots with no virtual topics,
/// default-deny RBAC and no client auth.
///
/// The composition root: [start] wires the four bounded components ([DynamicGatewayState],
/// [KafkaListener], [ClusterConnections], [AdminSurface]) together in the required order.
public final class KafkaGateway implements Gateway {

    private static final Logger log = LoggerFactory.getLogger(KafkaGateway.class);

    private final GatewayConfig config;
    private final MetadataCache cache = new MetadataCache();

    private GatewayMetrics metrics;
    private KafkaBodyCodec codec;
    private ApiVersionsResponseBuilder apiVersionsBuilder;
    private DynamicGatewayState dynamicState;
    private KafkaListener listener;
    private ClusterConnections connections;
    private AdminSurface admin;
    private KafkaTopicAdmin topicDescriber;

    private final AuthenticationProviderRegistry authenticationProviders;

    private volatile boolean running;

    /// A gateway with the authentication providers of the plugins on the classpath.
    public KafkaGateway(GatewayConfig config) {
        this(config, new PluginLoader(KafkaGateway.class.getClassLoader()).loadAll().authenticationProviders());
    }

    public KafkaGateway(GatewayConfig config, AuthenticationProviderRegistry authenticationProviders) {
        this.config = config;
        this.authenticationProviders = authenticationProviders;
    }

    @Override
    public void start() throws Exception {
        if (running) {
            log.warn("Cannot start Gateway because it is already running");
            return;
        }
        if (config.admin().enabled()) {
            // fail on an unknown admin.auth.type before connecting to anything
            authenticationProviders.require(config.admin().auth().type());
        }
        try {
            startComponents();
        } catch (Exception | Error e) {
            // a partially started gateway would otherwise keep the JVM alive with its Kafka threads
            closeComponents();
            throw e;
        }
    }

    private void startComponents() throws Exception {

        // Validate cluster, parse bootstrap
        var clusterCfg = config.defaultCluster();
        if (clusterCfg == null || clusterCfg.bootstrapServers().isEmpty()) {
            throw new IllegalStateException("No cluster configured");
        }
        String bootstrapServers = clusterCfg.bootstrapServers().getFirst();
        InetSocketAddress bootstrap = parseBootstrap(bootstrapServers);

        // How to reach the brokers (TLS and SASL), for the gateway's Netty connections and for its
        // own Kafka clients. Loads the TLS stores now, so a bad one fails before anything connects.
        var brokerAuth = config.auth().brokerAuth();
        var brokerConnections = BrokerConnectionSettings.of(clusterCfg, brokerAuth);
        var brokerSecurity = BrokerSecurity.clientProperties(clusterCfg.tls(), brokerAuth);
        log.info("connecting to the cluster over {}", BrokerSecurity.protocol(clusterCfg.tls(), brokerAuth));

        // How clients reach the gateway. Loads the listener's TLS stores now, for the same reason.
        var listenerConfig = config.listeners().getFirst();
        var listenerTls = listenerConfig.tls() == null ? null : ListenerTls.from(listenerConfig.tls());

        // Shared infrastructure
        metrics = new GatewayMetrics(new SimpleMeterRegistry());
        var kawaApiRegistry = KafkaApiRegistry.create();
        codec = new KafkaBodyCodec(kawaApiRegistry);
        apiVersionsBuilder = new ApiVersionsResponseBuilder(SupportedVersions.from(kawaApiRegistry));

        // Dynamic config from the config topic (blocks until caught up)
        dynamicState = new DynamicGatewayState(bootstrapServers, config.configTopic(), brokerSecurity);
        dynamicState.start();

        // Bind the client listener; resolve the advertised endpoint
        listener = new KafkaListener(metrics, codec, listenerConfig.securityProtocol(), listenerTls);
        int boundPort = listener.bind(listenerConfig.host(), listenerConfig.port());
        AdvertisedListener advertised = resolveAdvertised(config.advertised(), boundPort);

        // Interceptor pipeline
        topicDescriber = new KafkaTopicAdmin(bootstrapServers, brokerSecurity);
        RequestPipeline requestPipeline = buildPipeline(dynamicState, advertised, topicDescriber);

        // Broker connections (metadata client) - the same static bootstrap that hosts the
        //    config topic. The dynamic snapshot's `clusters` map is carried in the message but
        //    not used for routing in this milestone.
        connections = new ClusterConnections(codec, requestPipeline.pipeline(), metrics, cache,
                bootstrap.getHostString(), bootstrap.getPort(), brokerConnections);
        connections.start();

        // Dispatcher - installed only after the initial metadata fetch, so the listener
        //    (bound earlier for advertised-port resolution) closes every connection until the
        //    gateway can actually route.
        var dispatcher = new KafkaClientRequestHandler(
                codec, apiVersionsBuilder, requestPipeline.pipeline(),
                connections.router(), connections.pool(), connections.metadataClient(),
                metrics, requestPipeline.fetchSessions(), dynamicState.saslAuthenticator());
        listener.installDispatcher(dispatcher);

        //  Admin surface
        if (config.admin().enabled()) {
            admin = new AdminSurface(config.admin(), dynamicState, cache, bootstrapServers, brokerSecurity,
                    authenticationProviders);
            admin.start();
        }
        running = true;
        log.info("kawa gateway listening on {}:{} over {} (advertised as {}:{})",
                listenerConfig.host(), boundPort, listenerConfig.securityProtocol(), advertised.host(), advertised.port());
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        closeComponents();
    }

    private void closeComponents() {
        if (admin != null) {
            admin.close();
        }
        if (dynamicState != null) {
            dynamicState.close();
        }
        if (connections != null) {
            connections.close();
        }
        if (listener != null) {
            listener.close();
        }
        if (topicDescriber != null) {
            topicDescriber.close();
        }
    }

    public int boundPort() {
        return listener.boundPort();
    }

    /// The bound port of the admin HTTP server, or `-1` when the admin surface is disabled.
    public int adminBoundPort() {
        return admin == null ? -1 : admin.boundPort();
    }

    private RequestPipeline buildPipeline(DynamicGatewayState state, AdvertisedListener advertised, TopicDescriber topics) {
        var fetchSessions = new FetchSessionRegistry();
        if (state.modules().isEnabled(ModuleId.RBAC) && !state.authorizer().hasAnyAcls()) {
            log.warn("RBAC has no roles or groups configured - every request will be denied");
        }
        var governance = state.governance();
        var pipeline = InterceptorPipeline.builder()
                .module(ModuleId.RBAC, new AuthorizationInterceptor(state.authorizer(), state.virtualTopics()))
                .module(ModuleId.GOVERNANCE, new GovernanceInterceptor(() -> governance, topics, state.virtualTopics()::toPhysical))
                .module(ModuleId.VIRTUAL_TOPICS, new VirtualTopicInterceptor(state.virtualTopics(), fetchSessions))
                .always(new BrokerEndpointInterceptor(advertised))
                .build();
        state.modules().subscribe(pipeline::enable);
        state.modules().subscribe(KafkaGateway::warnWhenRbacIsDisabled);
        return new RequestPipeline(pipeline, fetchSessions);
    }

    /// RBAC is fail-open: switched off, every Kafka request is admitted without access control.
    private static void warnWhenRbacIsDisabled(Set<ModuleId> enabled) {
        if (!enabled.contains(ModuleId.RBAC)) {
            log.warn("RBAC disabled - no authorization is enforced on the Kafka path; "
                    + "requests are admitted without access control");
        }
    }

    private record RequestPipeline(InterceptorPipeline pipeline, FetchSessionRegistry fetchSessions) {
    }

    private static AdvertisedListener resolveAdvertised(
            AdvertisedListener advertised,
            int boundPort
    ) {
        Integer port = advertised.port() == null || advertised.port() == 0 ? boundPort : advertised.port();
        return AdvertisedListener.of(advertised.nodeId(), advertised.host(), port);
    }

    private static InetSocketAddress parseBootstrap(String server) {
        int scheme = server.indexOf("://");
        String hostPort = scheme >= 0 ? server.substring(scheme + 3) : server;
        int colon = hostPort.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("Invalid bootstrap server: " + server);
        }
        String host = hostPort.substring(0, colon);
        int port = Integer.parseInt(hostPort.substring(colon + 1));
        return new InetSocketAddress(host, port);
    }
}
