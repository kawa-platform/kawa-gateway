package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AclConfig;

import java.util.List;

/// A stored RBAC role in the admin `PUT /rbac/roles/{name}` response. Carries no name: the
/// caller read it off the path it just wrote.
///
/// @param acls the ACLs making up this role
public record RoleValueView(List<AclConfig> acls) {
}
