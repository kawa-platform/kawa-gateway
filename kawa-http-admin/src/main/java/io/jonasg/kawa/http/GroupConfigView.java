package io.jonasg.kawa.http;

import io.jonasg.kawa.config.ManagedBy;
import org.jspecify.annotations.Nullable;

import java.util.List;

/// A single RBAC group in the admin `/rbac/groups` response.
///
/// @param name    the group name
/// @param clients the usernames in this group
/// @param roles     the roles whose ACLs every client inherits
/// @param managedBy the owner of this group, or `null` when it is unmanaged
public record GroupConfigView(
        String name,
        List<String> clients,
        List<String> roles,
        @Nullable ManagedBy managedBy) {
}
