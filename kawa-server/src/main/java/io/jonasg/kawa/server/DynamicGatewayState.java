package io.jonasg.kawa.server;

import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.rbac.RbacAuthorizer;
import io.jonasg.kawa.server.auth.SaslAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/// Owns the four dynamically-reloaded consumers ([VirtualTopicManager], [RbacAuthorizer],
/// [SaslAuthenticator], [GovernancePolicy]) and the [DynamicConfigManager] that feeds them
/// from the config topic. Created empty, then [start] blocks until the config topic has been
/// caught up, so the gateway can refuse to serve until it has applied the config that existed
/// at boot. An empty config topic on first boot is valid - the gateway boots with no virtual
/// topics, default-deny RBAC and no client auth.
public final class DynamicGatewayState implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DynamicGatewayState.class);

    private final String bootstrapServers;
    private final String topic;
    private final VirtualTopicManager virtualTopics;
    private final RbacAuthorizer authorizer;
    private final SaslAuthenticator saslAuthenticator;
    private final GovernancePolicy governance;
    private final DynamicConfigManager configManager;

    public DynamicGatewayState(String bootstrapServers, String topic, BrokerAuthConfig brokerAuth) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        virtualTopics = new VirtualTopicManager(Map.of());
        authorizer = new RbacAuthorizer(new RbacConfig(Map.of(), Map.of()));
        saslAuthenticator = new SaslAuthenticator();
        governance = new GovernancePolicy(new GovernanceConfig(null, null));
        // Direct partition assignment: the config consumer re-reads the full topic from the
        // earliest offset on every boot, so no consumer group is needed (and none is
        // registered in the cluster).
        configManager = new DynamicConfigManager(
                bootstrapServers, topic,
                configTopicProps(brokerAuth),
                virtualTopics, authorizer, saslAuthenticator, governance);
    }

    /// Starts the config-topic consumer and blocks until the initial load has been applied.
    public void start() throws InterruptedException {
        configManager.start();
        log.info("waiting for initial config load from topic '{}' on {}", topic, bootstrapServers);
        configManager.awaitInitialLoad();
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

    public DynamicConfigManager configManager() {
        return configManager;
    }

    @Override
    public void close() {
        configManager.close();
    }

    /// Consumer properties for the config topic. The gateway's own broker connection is
    /// PLAIN-only (see `BrokerSaslAuthenticator`), so the config-topic consumer uses the
    /// same credentials when the broker requires SASL.
    private static Properties configTopicProps(BrokerAuthConfig brokerAuth) {
        Properties props = new Properties();
        if (brokerAuth != null) {
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.mechanism", brokerAuth.mechanism());
            props.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"" + brokerAuth.username() + "\" password=\"" + brokerAuth.password() + "\";");
        }
        return props;
    }
}
