package io.jonasg.kawa.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/// Role-based access control configuration: named roles carrying ACLs, and groups that
/// reference roles and list their clients. A user's effective ACLs are the union of every
/// role referenced by every group they belong to.
///
/// @param roles  named roles, each a list of ACLs
/// @param groups named groups, each a client list plus the roles those clients inherit
public record RbacConfig(
        Map<String, RoleConfig> roles,
        Map<String, GroupConfig> groups
) {

    public RbacConfig() {
        this(Map.of(), Map.of());
    }

    public RbacConfig {
        roles = roles == null ? Map.of() : Map.copyOf(roles);
        groups = groups == null ? Map.of() : Map.copyOf(groups);
    }

    /// Returns a new [RbacConfig] with the given role added or replaced.
    public RbacConfig upsertRole(String name, RoleConfig role) {
        var newRoles = new HashMap<>(roles);
        newRoles.put(name, role);
        return new RbacConfig(newRoles, groups);
    }

    /// Returns a new [RbacConfig] with the given role removed. The role is also stripped from
    /// every group that references it, so no group is left pointing at a missing role.
    public RbacConfig removeRole(String name) {
        var newRoles = new HashMap<>(roles);
        newRoles.remove(name);
        var newGroups = new HashMap<String, GroupConfig>();
        groups.forEach((groupName, group) -> {
            if (group.roles().contains(name)) {
                var remaining = new ArrayList<>(group.roles());
                remaining.removeIf(name::equals);
                newGroups.put(groupName, group.withRoles(remaining));
            } else {
                newGroups.put(groupName, group);
            }
        });
        return new RbacConfig(newRoles, newGroups);
    }

    /// Returns a new [RbacConfig] with the given group added or replaced.
    public RbacConfig upsertGroup(String name, GroupConfig group) {
        var newGroups = new HashMap<>(groups);
        newGroups.put(name, group);
        return new RbacConfig(roles, newGroups);
    }

    /// Returns a new [RbacConfig] with the given group removed.
    public RbacConfig removeGroup(String name) {
        var newGroups = new HashMap<>(groups);
        newGroups.remove(name);
        return new RbacConfig(roles, newGroups);
    }

    /// Returns a new [RbacConfig] with the group `from` re-keyed as `to`. The group's clients
    /// and roles move with it; no other entry references a group by name. A no-op when `from`
    /// does not exist.
    public RbacConfig renameGroup(String from, String to) {
        if (!groups.containsKey(from)) {
            return this;
        }
        var newGroups = new HashMap<>(groups);
        newGroups.put(to, newGroups.remove(from));
        return new RbacConfig(roles, newGroups);
    }
}
