package io.jonasg.kawa.config;

import tools.jackson.databind.annotation.JsonDeserialize;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/// The dynamic gateway configuration: everything that can change while the gateway runs.
///
/// Stored as full snapshots in the config topic, applied live by the gateway and changed through
/// the admin API. Startup-only settings (listeners, clusters, admin, ...) are [BootstrapConfig].
///
/// @param virtualTopics virtual topic config map
/// @param auth          client SASL authentication configuration
/// @param rbac          role-based access control configuration
/// @param governance    topic governance configuration
/// @param adminUsers    admin API users, keyed by their gateway-assigned UUID
/// @param modules       gateway modules switched on or off, keyed by [ModuleId#id]
public record DynamicConfig(
        @JsonDeserialize(using = VirtualTopicMapDeserializer.class) Map<String, VirtualTopicConfig> virtualTopics,
        AuthConfig auth,
        RbacConfig rbac,
        GovernanceConfig governance,
        Map<String, AdminUserConfig> adminUsers,
        Map<String, ModuleConfig> modules) {

    public DynamicConfig {
        if (virtualTopics == null) {
            virtualTopics = Map.of();
        }
        if (auth == null) {
            auth = new AuthConfig(null, null);
        }
        if (rbac == null) {
            rbac = new RbacConfig();
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
    }

    public DynamicConfig(
            Map<String, VirtualTopicConfig> virtualTopics,
            AuthConfig auth,
            RbacConfig rbac,
            GovernanceConfig governance) {
        this(virtualTopics, auth, rbac, governance, null);
    }

    public DynamicConfig(
            Map<String, VirtualTopicConfig> virtualTopics,
            AuthConfig auth,
            RbacConfig rbac,
            GovernanceConfig governance,
            Map<String, AdminUserConfig> adminUsers) {
        this(virtualTopics, auth, rbac, governance, adminUsers, null);
    }

    /// A fully-defaulted, empty dynamic config: no virtual topics, no RBAC, no client auth.
    /// Used as the base for the admin config endpoints before the first snapshot has been
    /// applied (an empty config topic on first boot).
    public static DynamicConfig empty() {
        return new DynamicConfig(null, null, null, null);
    }

    /// Convenience factory for programmatic configuration.
    public static DynamicConfig of(Map<String, String> virtualTopics, AuthConfig auth) {
        Map<String, VirtualTopicConfig> typedVirtualTopics = virtualTopics == null
                ? null
                : virtualTopics.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> new VirtualTopicConfig(entry.getValue())));
        return new DynamicConfig(typedVirtualTopics, auth, null, null);
    }

    /// Returns a new [DynamicConfig] with the [AuthConfig] section replaced.
    public DynamicConfig updateAuth(AuthConfig auth) {
        return copyWith(virtualTopics, auth, rbac, governance);
    }

    /// Returns a new [DynamicConfig] with the [RbacConfig] section replaced.
    public DynamicConfig updateRbac(RbacConfig rbac) {
        return copyWith(virtualTopics, auth, rbac, governance);
    }

    public DynamicConfig upsertGovernanceRule(GovernanceRuleConfig governanceRuleConfig) {
        var governance = governance().upsertRule(governanceRuleConfig.name(), governanceRuleConfig);
        return copyWith(virtualTopics, auth, rbac, governance);
    }

    /// Returns a new [DynamicConfig] with the governance rule `name` removed.
    public DynamicConfig removeGovernanceRule(String name) {
        return copyWith(virtualTopics, auth, rbac, governance().removeRule(name));
    }

    /// Returns a new [DynamicConfig] with the given global governance exemption added or replaced.
    public DynamicConfig upsertGovernanceExemption(GovernanceRuleConfig.Exemption exemption) {
        return copyWith(virtualTopics, auth, rbac, governance().upsertExemption(exemption.name(), exemption));
    }

    /// Returns a new [DynamicConfig] with the governance section replaced.
    public DynamicConfig updateGovernance(GovernanceConfig governance) {
        return copyWith(virtualTopics, auth, rbac, governance);
    }

    /// Returns a new [DynamicConfig] with the global governance exemption `name` removed.
    public DynamicConfig removeGovernanceExemption(String name) {
        return copyWith(virtualTopics, auth, rbac, governance().removeExemption(name));
    }

    /// Returns a new [DynamicConfig] with the virtual topics map replaced.
    public DynamicConfig updateVirtualTopics(Map<String, VirtualTopicConfig> virtualTopics) {
        return copyWith(virtualTopics, auth, rbac, governance);
    }

    /// Returns a new [DynamicConfig] with the given virtual topic added or replaced.
    public DynamicConfig upsertVirtualTopic(String topicName, VirtualTopicConfig topic) {
        var newTopics = new HashMap<>(virtualTopics);
        newTopics.put(topicName, topic);
        return copyWith(Map.copyOf(newTopics), auth, rbac, governance);
    }

    /// Returns a new [DynamicConfig] with the given virtual topic removed.
    public DynamicConfig removeVirtualTopic(String topicName) {
        var newTopics = new HashMap<>(virtualTopics);
        newTopics.remove(topicName);
        return copyWith(Map.copyOf(newTopics), auth, rbac, governance);
    }

    /// Returns a new [DynamicConfig] with the admin user `id` added or replaced.
    public DynamicConfig upsertAdminUser(String id, AdminUserConfig user) {
        var users = new HashMap<>(adminUsers);
        users.put(id, user);
        return withAdminUsers(Map.copyOf(users));
    }

    /// Returns a new [DynamicConfig] with the admin user `id` removed.
    public DynamicConfig removeAdminUser(String id) {
        var users = new HashMap<>(adminUsers);
        users.remove(id);
        return withAdminUsers(Map.copyOf(users));
    }

    /// Returns a new [DynamicConfig] with the given module added or replaced.
    public DynamicConfig upsertModule(ModuleConfig module) {
        var newModules = new HashMap<>(modules);
        newModules.put(module.id().id(), module);
        return withModules(Map.copyOf(newModules));
    }

    private DynamicConfig withAdminUsers(Map<String, AdminUserConfig> adminUsers) {
        return new DynamicConfig(virtualTopics, auth, rbac, governance, adminUsers, modules);
    }

    private DynamicConfig withModules(Map<String, ModuleConfig> modules) {
        return new DynamicConfig(virtualTopics, auth, rbac, governance, adminUsers, modules);
    }

    private DynamicConfig copyWith(
            Map<String, VirtualTopicConfig> virtualTopics,
            AuthConfig auth,
            RbacConfig rbac,
            GovernanceConfig governance
    ) {
        return new DynamicConfig(virtualTopics, auth, rbac, governance, adminUsers, modules);
    }
}
