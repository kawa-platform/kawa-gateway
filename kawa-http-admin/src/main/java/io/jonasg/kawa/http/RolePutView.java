package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.ManagedBy;
import org.jspecify.annotations.Nullable;

import java.util.List;

/// A stored RBAC role in the admin `PUT /rbac/roles/{name}` response. Carries no name: the
/// caller read it off the path it just wrote.
///
/// @param acls      the ACLs making up this role
/// @param managedBy the owner of this role, or `null` when it is unmanaged
public record RolePutView(List<AclConfig> acls, @Nullable ManagedBy managedBy) {
}
