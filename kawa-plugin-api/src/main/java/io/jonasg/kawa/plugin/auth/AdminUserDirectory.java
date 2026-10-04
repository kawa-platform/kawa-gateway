package io.jonasg.kawa.plugin.auth;

import java.util.Optional;

/// kawa's local admin users: the bootstrap user from the gateway config file plus the admin users
/// stored in the config topic (managed through `/admin/users`).
public interface AdminUserDirectory {

    /// Whether `password` is the password of the admin user `username`. Costs the same for unknown
    /// users, so response times do not reveal which usernames exist.
    boolean verify(String username, String password);

    /// Whether the admin user `username` currently exists.
    boolean exists(String username);

    /// The bootstrap user's name, if one is configured.
    Optional<String> bootstrapUsername();
}
