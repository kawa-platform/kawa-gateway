package io.jonasg.kawa.http;

/// A single admin user in the `/admin/users` responses. Never carries the password hash.
///
/// @param username the admin username
public record AdminUserView(String username) {
}
