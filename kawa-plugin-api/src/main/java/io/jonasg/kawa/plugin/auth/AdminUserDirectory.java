package io.jonasg.kawa.plugin.auth;

import java.util.Optional;

/// kawa's local admin users: the admin users stored in the config topic (managed through
/// `/admin/users`). The bootstrap user from the gateway config file is one of them once the gateway
/// has seeded it on first start; there is no separate in-memory account.
///
/// An admin user logs in with their username (their email address, case-insensitive) but is
/// identified by a stable **subject**, the value an authentication provider puts in the tokens it
/// issues: their gateway-assigned id, so their tokens survive a change of email.
public interface AdminUserDirectory {

    /// The subject of the enabled admin user `username` if `password` is their password. Costs the
    /// same for unknown and disabled users, so response times do not reveal which usernames exist.
    Optional<String> authenticate(String username, String password);

    /// The current username of the enabled admin user with this `subject`, or empty when they were
    /// deleted or disabled. Checked for every token, so deleting or disabling an admin user revokes
    /// their tokens immediately.
    Optional<String> username(String subject);

    /// Whether at least one admin user is stored. A provider that only admits kawa's local admin
    /// users needs one, or nobody could ever log in.
    boolean hasAdminUsers();
}
