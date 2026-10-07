package io.jonasg.kawa.http;

import org.jspecify.annotations.NullUnmarked;

/// The body of `POST /admin/users`. Only these fields are accepted: `enabled`, `createdAt` and the id
/// are the gateway's to set, so a request carrying them is rejected.
///
/// @param email       the email address the admin user logs in with; required
/// @param password    the plaintext password, hashed before it enters the config model; required
/// @param displayName an optional human-readable label
@NullUnmarked
public record CreateAdminUserRequest(String email, String password, String displayName) {
}
