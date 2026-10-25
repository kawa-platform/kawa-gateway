package io.jonasg.kawa.http;

import io.jonasg.kawa.config.ManagedBy;
import org.jspecify.annotations.Nullable;

import java.util.List;

/// A stored RBAC group in the admin `PUT /rbac/groups/{name}` response. Carries no name: the
/// caller read it off the path it just wrote.
///
/// @param clients the usernames in this group
/// @param roles     the roles whose ACLs every client inherits
/// @param managedBy the owner of this group, or `null` when it is unmanaged
public record GroupPutView(List<String> clients, List<String> roles, @Nullable ManagedBy managedBy) {
}
