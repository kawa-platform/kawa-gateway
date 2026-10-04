package io.jonasg.kawa.http;

import org.jspecify.annotations.NullUnmarked;

/// An admin user upsert request.
///
/// @param password the plaintext password; hashed before it enters the config model
@NullUnmarked
public record AdminUserRequest(String password) {
}
