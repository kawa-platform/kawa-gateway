package io.jonasg.kawa.http;

import org.jspecify.annotations.NullUnmarked;

/// The body of `PATCH /admin/users/{id}`. Every field is optional and a field left out keeps its
/// value; any other field is rejected.
///
/// @param email       a new email address to log in with
/// @param password    a new plaintext password; must not be blank
/// @param displayName a new human-readable label; an empty value clears it
/// @param enabled     switches the admin user on or off
@NullUnmarked
public record UpdateAdminUserRequest(String email, String password, String displayName, Boolean enabled) {
}
