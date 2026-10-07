package io.jonasg.kawa.http;

import org.jspecify.annotations.Nullable;

import java.time.Instant;

/// A single admin user in the `/admin/users` responses. Never carries the password or its hash.
///
/// @param id          the gateway-assigned UUID that addresses the admin user
/// @param email       the email address the admin user logs in with, in lower case
/// @param displayName the optional human-readable label
/// @param enabled     whether the admin user is switched on
/// @param createdAt   when the gateway created the admin user
public record AdminUserView(String id, String email, @Nullable String displayName, boolean enabled, Instant createdAt) {
}
