package io.jonasg.kawa.config;

import java.util.List;

/// A named group: the clients it contains and the roles those clients inherit.
///
/// @param clients   the usernames in this group
/// @param roles     the roles whose ACLs every client inherits
/// @param managedBy the owner of this entry, or `null` when it is unmanaged
public record GroupConfig(List<String> clients, List<String> roles, ManagedBy managedBy) {

    public GroupConfig {
        clients = clients == null ? List.of() : List.copyOf(clients);
        roles = roles == null ? List.of() : List.copyOf(roles);
    }

    public GroupConfig(List<String> clients, List<String> roles) {
        this(clients, roles, null);
    }

    public GroupConfig removeClient(String name) {
        var withRemovedClient = this.clients.stream()
                .filter(client -> !client.equals(name))
                .toList();
        return withClients(withRemovedClient);
    }

    /// Returns a copy with `clients` as its members, keeping its roles and owner.
    public GroupConfig withClients(List<String> clients) {
        return new GroupConfig(clients, roles, managedBy);
    }

    /// Returns a copy with `roles` as its roles, keeping its members and owner.
    public GroupConfig withRoles(List<String> roles) {
        return new GroupConfig(clients, roles, managedBy);
    }

    /// Returns a copy owned by `managedBy` (`null` for unmanaged).
    public GroupConfig withManagedBy(ManagedBy managedBy) {
        return new GroupConfig(clients, roles, managedBy);
    }
}
