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
/// @param adminUsers    admin API users, keyed by their gateway-assigned UUID
/// @param modules       gateway modules switched on or off, keyed by [ModuleId#id]
public record GatewayConfig(
        List<ListenerConfig> listeners,
        Map<String, ClusterConfig> clusters,
        @JsonDeserialize(using = VirtualTopicMapDeserializer.class) Map<String, VirtualTopicConfig> virtualTopics,
        AdvertisedListener advertised,
        AuthConfig auth,
        RbacConfig rbac,
        AdminConfig admin,
        String configTopic,
        GovernanceConfig governance,
        Map<String, AdminUserConfig> adminUsers,
        Map<String, ModuleConfig> modules) {

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
        if (adminUsers == null) {
            adminUsers = Map.of();
        }
        if (modules == null) {
            modules = Map.of();
        }
        if (advertised == null) {
            ListenerConfig first = listeners.getFirst();
            advertised = new AdvertisedListener(null, null, first.port());
        }
    }

    public GatewayConfig(
            List<ListenerConfig> listeners,
            Map<String, ClusterConfig> clusters,
            Map<String, VirtualTopicConfig> virtualTopics,
            AdvertisedListener advertised,
            AuthConfig auth,
            RbacConfig rbac,
            AdminConfig admin,
            String configTopic,
            GovernanceConfig governance) {
        this(listeners, clusters, virtualTopics, advertised, auth, rbac, admin, configTopic, governance, null);
    }

    public GatewayConfig(
            List<ListenerConfig> listeners,
            Map<String, ClusterConfig> clusters,
            Map<String, VirtualTopicConfig> virtualTopics,
            AdvertisedListener advertised,
            AuthConfig auth,
            RbacConfig rbac,
            AdminConfig admin,
            String configTopic,
            GovernanceConfig governance,
            Map<String, AdminUserConfig> adminUsers) {
        this(listeners, clusters, virtualTopics, advertised, auth, rbac, admin, configTopic, governance, adminUsers, null);
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

    /// Returns a new [GatewayConfig] with the governance rule `name` removed.
    public GatewayConfig removeGovernanceRule(String name) {
        return copyWith(virtualTopics, auth, rbac, governance().removeRule(name));
    }

    /// Returns a new [GatewayConfig] with the given global governance exemption added or replaced.
    public GatewayConfig upsertGovernanceExemption(GovernanceRuleConfig.Exemption exemption) {
        return copyWith(virtualTopics, auth, rbac, governance().upsertExemption(exemption.name(), exemption));
    }

    /// Returns a new [GatewayConfig] with the governance section replaced.
    public GatewayConfig updateGovernance(GovernanceConfig governance) {
        return copyWith(virtualTopics, auth, rbac, governance);
    }

    /// Returns a new [GatewayConfig] with the global governance exemption `name` removed.
    public GatewayConfig removeGovernanceExemption(String name) {
        return copyWith(virtualTopics, auth, rbac, governance().removeExemption(name));
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

    /// Returns a new [GatewayConfig] with the admin user `id` added or replaced.
    public GatewayConfig upsertAdminUser(String id, AdminUserConfig user) {
        var users = new HashMap<>(adminUsers);
        users.put(id, user);
        return withAdminUsers(Map.copyOf(users));
    }

    /// Returns a new [GatewayConfig] with the admin user `id` removed.
    public GatewayConfig removeAdminUser(String id) {
        var users = new HashMap<>(adminUsers);
        users.remove(id);
        return withAdminUsers(Map.copyOf(users));
    }

    /// Returns a new [GatewayConfig] with the given module added or replaced.
    public GatewayConfig upsertModule(ModuleConfig module) {
        var newModules = new HashMap<>(modules);
        newModules.put(module.id().id(), module);
        return withModules(Map.copyOf(newModules));
    }

    private GatewayConfig withAdminUsers(Map<String, AdminUserConfig> adminUsers) {
        return new GatewayConfig(
                listeners, clusters, virtualTopics, advertised, auth, rbac, admin, configTopic, governance, adminUsers, modules);
    }

    private GatewayConfig withModules(Map<String, ModuleConfig> modules) {
        return new GatewayConfig(
                listeners, clusters, virtualTopics, advertised, auth, rbac, admin, configTopic, governance, adminUsers, modules);
    }

    private GatewayConfig copyWith(
            Map<String, VirtualTopicConfig> virtualTopics,
            AuthConfig auth,
            RbacConfig rbac,
            GovernanceConfig governance
    ) {
        return new GatewayConfig(
                listeners, clusters, virtualTopics, advertised, auth, rbac, admin, configTopic, governance, adminUsers, modules);
    }
}
