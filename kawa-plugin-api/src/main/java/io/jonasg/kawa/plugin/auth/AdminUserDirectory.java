package io.jonasg.kawa.plugin.auth;

import java.util.Optional;

/// kawa's local admin users: the bootstrap user from the gateway config file plus the admin users
/// stored in the config topic (managed through `/admin/users`).
///
/// An admin user logs in with their username (their email address, case-insensitive) but is
/// identified by a stable **subject**, the value an authentication provider puts in the tokens it
/// issues. A stored admin user's subject is their gateway-assigned id, so their tokens survive a
/// change of email; the bootstrap user's subject is their username.
public interface AdminUserDirectory {

    /// The subject of the enabled admin user `username` if `password` is their password. Costs the
    /// same for unknown and disabled users, so response times do not reveal which usernames exist.
    Optional<String> authenticate(String username, String password);

    /// The current username of the enabled admin user with this `subject`, or empty when they were
    /// deleted or disabled. Checked for every token, so deleting or disabling an admin user revokes
    /// their tokens immediately.
    Optional<String> username(String subject);

    /// The bootstrap user's name, if one is configured.
    Optional<String> bootstrapUsername();
}
