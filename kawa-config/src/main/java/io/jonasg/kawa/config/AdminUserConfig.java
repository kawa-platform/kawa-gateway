package io.jonasg.kawa.config;

import org.jspecify.annotations.Nullable;

import java.time.Instant;

/// An admin API user stored in the config topic, keyed by a gateway-assigned UUID. The email is the
/// username they log in with: stored in lower case so it is unique regardless of case. The password
/// is kept as a PBKDF2-HMAC-SHA256 hash (the PLAIN encoding of [HashedPassword]); plaintext never
/// enters the config model.
///
/// @param email       the email address the admin user logs in with, normalized to lower case
/// @param password    the hashed password
/// @param displayName an optional human-readable label to tell admin users apart
/// @param enabled     whether the admin user is switched on; switched off rather than deleted
/// @param createdAt   when the gateway created the admin user; never client-supplied
public record AdminUserConfig(
        String email,
        HashedPassword password,
        @Nullable String displayName,
        boolean enabled,
        Instant createdAt
) {

    public AdminUserConfig {
        email = EmailAddresses.normalize(email);
        if (password == null) {
            throw new IllegalArgumentException("password must not be null");
        }
        if (createdAt == null) {
            throw new IllegalArgumentException("createdAt must not be null");
        }
    }
}
