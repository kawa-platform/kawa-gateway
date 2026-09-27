package io.jonasg.kawa.http;

import java.util.List;

/// A stored RBAC group in the admin `PUT /rbac/groups/{name}` response. Carries no name: the
/// caller read it off the path it just wrote.
///
/// @param clients the usernames in this group
/// @param roles   the roles whose ACLs every client inherits
public record GroupPutView(List<String> clients, List<String> roles) {
}
