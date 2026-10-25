package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.ManagedBy;
import org.jspecify.annotations.Nullable;

import java.util.List;

/// A single RBAC role in the admin `/rbac/roles` response.
///
/// @param name the role name
/// @param acls      the ACLs making up this role
/// @param managedBy the owner of this role, or `null` when it is unmanaged
public record RoleConfigView(
        String name,
        List<AclConfig> acls,
        @Nullable ManagedBy managedBy) {
}
