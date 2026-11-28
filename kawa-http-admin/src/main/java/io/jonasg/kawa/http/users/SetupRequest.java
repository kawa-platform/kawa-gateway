package io.jonasg.kawa.http.users;

import org.jspecify.annotations.NullUnmarked;

/// The body of `POST /admin/setup`. Only these fields are accepted.
///
/// @param token    the setup token the gateway logged at startup, or `KAWA_SETUP_TOKEN`; required
/// @param email    the email address the first admin user logs in with; required
/// @param password the plaintext password, hashed before it enters the config model; required
@NullUnmarked
record SetupRequest(String token, String email, String password) {
}
