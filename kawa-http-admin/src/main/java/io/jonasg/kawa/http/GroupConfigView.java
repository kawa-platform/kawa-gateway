package io.jonasg.kawa.http;

import java.util.List;

/// A single RBAC group in the admin `/rbac/groups` response.
///
/// @param name    the group name
/// @param clients the usernames in this group
/// @param roles   the roles whose ACLs every client inherits
public record GroupConfigView(
        String name,
        List<String> clients,
        List<String> roles) {
}
