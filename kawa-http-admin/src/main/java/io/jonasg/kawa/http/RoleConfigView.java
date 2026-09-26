package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AclConfig;

import java.util.List;

/// A single RBAC role in the admin `/rbac/roles` response.
///
/// @param name the role name
/// @param acls the ACLs making up this role
public record RoleConfigView(
        String name,
        List<AclConfig> acls) {
}
