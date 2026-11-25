package io.jonasg.kawa.rbac;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/// Resolves the RBAC configuration into a per-user ACL list, then answers authorization queries
/// against that map. Default-deny: a request is authorized only if at least one matching ACL
/// allows it, and any matching deny wins immediately.
///
/// The per-user ACL map is an immutable snapshot replaced atomically via [reload]: a reader on
/// the hot path sees either the previous or the new ACL set, never a partially-applied one.
public final class RbacAuthorizer {

    private volatile Map<String, List<AclConfig>> aclsByUser;

    public RbacAuthorizer(RbacConfig config) {
        reload(config);
    }

    /// Replaces the per-user ACL map with a new snapshot built from `config`: [#publish] of
    /// [#prepare]. An unknown role reference throws and leaves the previous snapshot in place.
    public void reload(RbacConfig config) {
        publish(prepare(config));
    }

    /// Resolves `config` into the per-user ACL map without changing what this authorizer answers.
    /// Pass it to [#publish] to make it current.
    ///
    /// @throws IllegalArgumentException if `config` references an unknown role
    public Snapshot prepare(RbacConfig config) {
        return new Snapshot(flatten(config));
    }

    /// Makes `snapshot`, built by [#prepare], the current ACL set.
    ///
    /// Safe to call concurrently with readers: the map is assigned to a single `volatile`
    /// reference, so readers see either the previous or the new ACL set, never a partially-applied one.
    public void publish(Snapshot snapshot) {
        this.aclsByUser = snapshot.aclsByUser;
    }

    /// A resolved RBAC config, built by [#prepare].
    public static final class Snapshot {

        private final Map<String, List<AclConfig>> aclsByUser;

        private Snapshot(Map<String, List<AclConfig>> aclsByUser) {
            this.aclsByUser = aclsByUser;
        }
    }

    /// Whether any principal has any ACLs configured. When `false`, every request is denied
    /// (default-deny with an empty config).
    public boolean hasAnyAcls() {
        return !aclsByUser.isEmpty();
    }

    /// Whether `principal` is authorized to perform `operation` on `resource` of `type`.
    ///
    /// Default-deny: `false` unless at least one matching ACL allows it. Any matching
    /// [AclPermissionType#DENY] ACL denies immediately, regardless of what else matches.
    public boolean isAuthorized(String principal, ResourceType type, String resource, AclOperation operation) {
        boolean allowed = false;
        for (AclConfig acl : aclsFor(principal)) {
            if (!matches(acl, type, resource, operation)) {
                continue;
            }
            if (acl.permission() == AclPermissionType.DENY) {
                return false; // deny wins, stop immediately
            }
            allowed = true;
        }
        return allowed; // false if nothing matched at all - default deny
    }

    private List<AclConfig> aclsFor(String principal) {
        return aclsByUser.getOrDefault(principal, List.of());
    }

    private static Map<String, List<AclConfig>> flatten(RbacConfig config) {
        Map<String, List<AclConfig>> byUser = new HashMap<>();
        config.groups().forEach((groupName, group) -> {
            List<AclConfig> groupAcls = group.roles().stream()
                    .flatMap(roleName -> {
                        var role = config.roles().get(roleName);
                        if (role == null) {
                            throw new IllegalArgumentException(
                                    "group '" + groupName + "' references unknown role '" + roleName + "'");
                        }
                        return role.acls().stream();
                    })
                    .toList();
            group.clients().forEach(client ->
                    byUser.computeIfAbsent(client, k -> new ArrayList<>()).addAll(groupAcls));
        });
        return byUser.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> List.copyOf(entry.getValue())));
    }

    private static boolean matches(AclConfig acl, ResourceType type, String resource, AclOperation operation) {
        ResourceConfig res = acl.resource();
        if (res.type() != type) {
            return false;
        }
        if (operation != acl.operation() && acl.operation() != AclOperation.ALL) {
            return false;
        }
        if (res.type() == ResourceType.CLUSTER) {
            return true; // pattern is null; matches the cluster regardless of name
        }
        String pattern = res.pattern();
        if (res.patternType() == PatternType.PREFIXED) {
            return resource.startsWith(pattern);
        }
        return resource.equals(pattern); // LITERAL exact match
    }
}
