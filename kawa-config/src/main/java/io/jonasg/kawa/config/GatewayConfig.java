package io.jonasg.kawa.config;

import tools.jackson.databind.annotation.JsonDeserialize;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/// Immutable gateway configuration.
///
/// @param listeners     client-facing listeners
/// @param clusters      upstream Kafka clusters
/// @param virtualTopics virtual topic config map
/// @param advertised    the endpoint advertised to clients in rewritten metadata
/// @param auth          client SASL authentication configuration
/// @param rbac          role-based access control configuration
/// @param admin         admin HTTP listener configuration
/// @param configTopic   the topic that holds the dynamic gateway config (defaults to `__kawa`)
/// @param governance    topic governance configuration
public record GatewayConfig(
        List<ListenerConfig> listeners,
        Map<String, ClusterConfig> clusters,
        @JsonDeserialize(using = VirtualTopicMapDeserializer.class) Map<String, VirtualTopicConfig> virtualTopics,
        AdvertisedListener advertised,
        AuthConfig auth,
        RbacConfig rbac,
        AdminConfig admin,
        String configTopic,
        GovernanceConfig governance) {

    public GatewayConfig {
        if (listeners == null) {
            listeners = List.of(new ListenerConfig(null, 9092));
        }
        if (clusters == null) {
            clusters = Map.of();
        }
        if (virtualTopics == null) {
            virtualTopics = Map.of();
        }
        if (auth == null) {
            auth = new AuthConfig(null, null, null);
        }
        if (rbac == null) {
            rbac = new RbacConfig();
        }
        if (admin == null) {
            admin = new AdminConfig(false, null, null, null);
        }
        if (configTopic == null) {
            configTopic = "__kawa";
        }
        if (governance == null) {
            governance = new GovernanceConfig(null);
        }
        if (advertised == null) {
            ListenerConfig first = listeners.getFirst();
            advertised = new AdvertisedListener(null, null, first.port());
        }
    }

    /// A fully-defaulted, empty gateway config: no virtual topics, no RBAC, no client auth.
    /// Used as the base for the admin config endpoints before the first snapshot has been
    /// applied (an empty config topic on first boot).
    public static GatewayConfig empty() {
        return new GatewayConfig(null, null, null, null, null, null, null, null, null);
    }

    /// Convenience factory for programmatic configuration.
    public static GatewayConfig of(
            List<ListenerConfig> listeners,
            Map<String, ClusterConfig> clusters,
            Map<String, String> virtualTopics,
            AdvertisedListener advertised,
            AuthConfig auth) {
        Map<String, VirtualTopicConfig> typedVirtualTopics = virtualTopics == null
                ? null
                : virtualTopics.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> new VirtualTopicConfig(entry.getValue())));
        return new GatewayConfig(listeners, clusters, typedVirtualTopics, advertised, auth, null, null, null, null);
    }

    /// The default cluster (first entry), or `null` if none is configured.
    public ClusterConfig defaultCluster() {
        if (clusters.isEmpty()) {
            return null;
        }
        return clusters.values().iterator().next();
    }

    /// Returns a new [GatewayConfig] with the [AuthConfig] section replaced.
    public GatewayConfig updateAuth(AuthConfig auth) {
        return copyWith(virtualTopics, auth, rbac, governance);
    }

    /// Returns a new [GatewayConfig] with the [RbacConfig] section replaced.
    public GatewayConfig updateRbac(RbacConfig rbac) {
        return copyWith(virtualTopics, auth, rbac, governance);
    }

    public GatewayConfig upsertGovernanceRule(GovernanceRuleConfig governanceRuleConfig) {
        var governance = governance().upsertRule(governanceRuleConfig.name(), governanceRuleConfig);
        return copyWith(virtualTopics, auth, rbac, governance);
    }

    /// Returns a new [GatewayConfig] with the virtual topics map replaced.
    public GatewayConfig updateVirtualTopics(Map<String, VirtualTopicConfig> virtualTopics) {
        return copyWith(virtualTopics, auth, rbac, governance);
    }

    /// Returns a new [GatewayConfig] with the given virtual topic added or replaced.
    public GatewayConfig upsertVirtualTopic(String topicName, VirtualTopicConfig topic) {
        var newTopics = new HashMap<>(virtualTopics);
        newTopics.put(topicName, topic);
        return copyWith(Map.copyOf(newTopics), auth, rbac, governance);
    }

    /// Returns a new [GatewayConfig] with the given virtual topic removed.
    public GatewayConfig removeVirtualTopic(String topicName) {
        var newTopics = new HashMap<>(virtualTopics);
        newTopics.remove(topicName);
        return copyWith(Map.copyOf(newTopics), auth, rbac, governance);
    }

    private GatewayConfig copyWith(
            Map<String, VirtualTopicConfig> virtualTopics,
            AuthConfig auth,
            RbacConfig rbac,
            GovernanceConfig governance
    ) {
        return new GatewayConfig(
                listeners, clusters, virtualTopics, advertised, auth, rbac, admin, configTopic, governance);
    }
}
