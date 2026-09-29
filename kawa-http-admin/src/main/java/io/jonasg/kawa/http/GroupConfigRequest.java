package io.jonasg.kawa.http;

import org.jspecify.annotations.NullUnmarked;

import java.util.List;

/// A group upsert request body.
///
/// @param clients the usernames in this group
/// @param roles   the roles whose ACLs every client inherits
@NullUnmarked
public record GroupConfigRequest(List<String> clients, List<String> roles) {

    public GroupConfigRequest {
        clients = clients == null ? List.of() : List.copyOf(clients);
        roles = roles == null ? List.of() : List.copyOf(roles);
    }
}
