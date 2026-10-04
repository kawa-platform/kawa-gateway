package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminUserConfig;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

/// Converts between the `/admin/users` transport types and [AdminUserConfig], and owns all input
/// validation for admin user requests.
///
/// A stateless instance created once in [AdminHttpServer] and shared by the admin user handlers.
@NullUnmarked
final class AdminUserMapper {

    private final @Nullable String bootstrapUsername;

    /// @param bootstrapUsername the bootstrap admin from the YAML config, which cannot be managed here
    AdminUserMapper(@Nullable String bootstrapUsername) {
        this.bootstrapUsername = bootstrapUsername;
    }

    /// The admin user to store for `PUT /admin/users/{name}`.
    AdminUserConfig toAdminUserConfig(String name, AdminUserRequest request) {
        if (name.equals(bootstrapUsername)) {
            throw new IllegalArgumentException(
                    "admin user '" + name + "' is the bootstrap user and is managed in the gateway config file");
        }
        if (request == null || request.password() == null || request.password().isBlank()) {
            throw new IllegalArgumentException("admin user '" + name + "': password is required");
        }
        return AdminUserConfig.fromPlaintext(request.password());
    }

    /// The admin user as the response body.
    AdminUserView toAdminUserView(String name) {
        return new AdminUserView(name);
    }
}
