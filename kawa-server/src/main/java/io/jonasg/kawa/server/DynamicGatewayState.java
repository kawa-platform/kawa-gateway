package io.jonasg.kawa.server;

import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistries;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.rbac.RbacAuthorizer;
import io.jonasg.kawa.server.auth.SaslAuthenticator;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/// Owns the dynamically-reloaded consumers ([VirtualTopicManager], [RbacAuthorizer],
/// [SaslAuthenticator], [GovernancePolicy]) and the [DynamicConfigManager] that feeds them, and the
/// Kafka path's [InterceptorPipeline], from the config topic. Created empty, so the pipeline can be
/// built on its consumers; then [start] blocks until the config topic has been caught up, so the
/// gateway can refuse to serve until it has applied the config that existed at boot. An empty
/// config topic on first boot is valid - the gateway boots with no virtual topics, default-deny
/// RBAC and no client auth.
public final class DynamicGatewayState implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DynamicGatewayState.class);

    private final String bootstrapServers;
    private final String topic;
    private final Properties brokerSecurity;
    private final VirtualTopicManager virtualTopics;
    private final RbacAuthorizer authorizer;
    private final SaslAuthenticator saslAuthenticator;
    private final GovernancePolicy governance;
    private volatile @Nullable DynamicConfigManager configManager;

    /// State for a cluster without a schema registry.
    public DynamicGatewayState(String bootstrapServers, String topic, Properties brokerSecurity) {
        this(bootstrapServers, topic, brokerSecurity, SchemaRegistries.NONE);
    }

    /// @param brokerSecurity   the Kafka client settings for reaching the brokers (`security.protocol`,
    ///                         `sasl.*`, `ssl.*`), see [io.jonasg.kawa.config.BrokerSecurity#clientProperties]
    /// @param schemaRegistries the schema registry of each physical topic's cluster, which every applied
    ///                         snapshot's virtual topics decode and encode Avro values with
    public DynamicGatewayState(String bootstrapServers, String topic, Properties brokerSecurity, SchemaRegistries schemaRegistries) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.brokerSecurity = brokerSecurity;
        virtualTopics = new VirtualTopicManager(Map.of(), schemaRegistries);
        authorizer = new RbacAuthorizer(new RbacConfig(Map.of(), Map.of()));
        saslAuthenticator = new SaslAuthenticator();
        governance = new GovernancePolicy(new GovernanceConfig(null));
    }

    /// Starts the config-topic consumer and blocks until the initial load has been applied.
    ///
    /// @param pipeline the Kafka path's interceptors, built on this state's consumers; each applied
    ///                 snapshot switches its modules
    public void start(InterceptorPipeline pipeline) throws InterruptedException {
        var configManager = createConfigManager(pipeline);
        configManager.start();
        log.info("waiting for initial config load from topic '{}' on {}", topic, bootstrapServers);
        configManager.awaitInitialLoad();
    }

    /// Creates the config manager that feeds this state's consumers and switches `pipeline`'s modules,
    /// without starting its consumer. Package-private so tests can apply snapshots without a broker.
    DynamicConfigManager createConfigManager(InterceptorPipeline pipeline) {
        // Direct partition assignment: the config consumer re-reads the full topic from the
        // earliest offset on every boot, so no consumer group is needed (and none is
        // registered in the cluster).
        configManager = new DynamicConfigManager(
                bootstrapServers, topic,
                brokerSecurity,
                virtualTopics, authorizer, saslAuthenticator, governance, pipeline);
        return configManager;
    }

    public VirtualTopicManager virtualTopics() {
        return virtualTopics;
    }

    public RbacAuthorizer authorizer() {
        return authorizer;
    }

    public SaslAuthenticator saslAuthenticator() {
        return saslAuthenticator;
    }

    public GovernancePolicy governance() {
        return governance;
    }

    /// The config manager. Only after [#start].
    public DynamicConfigManager configManager() {
        if (configManager == null) {
            throw new IllegalStateException("the dynamic gateway state has not been started");
        }
        return configManager;
    }

    /// Whether the initial config load has been applied. False until [#start] has created the config manager.
    public boolean isConfigLoaded() {
        DynamicConfigManager manager = configManager;
        return manager != null && manager.initialLoadComplete();
    }

    @Override
    public void close() {
        if (configManager != null) {
            configManager.close();
        }
    }
}
