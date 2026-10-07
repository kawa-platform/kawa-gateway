package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminUserConfig;
import io.jonasg.kawa.config.EmailAddresses;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.Mechanism;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import java.time.Clock;

/// Converts between the `/admin/users` transport types and [AdminUserConfig], and validates a new
/// admin user. Updates, and every check against the other admin users such as a unique email,
/// belong to [AdminUserService], which runs them on the config snapshot it writes.
///
/// A stateless instance created once in [AdminHttpServer] and shared by the admin user handlers.
@NullUnmarked
final class AdminUserMapper {

    private final Clock clock;

    /// @param clock assigns `createdAt` to new admin users
    AdminUserMapper(Clock clock) {
        this.clock = clock;
    }

    /// The admin user to create for `POST /admin/users`: enabled, created now.
    AdminUserConfig toNewAdminUser(CreateAdminUserRequest request) {
        if (request == null || request.email() == null || request.email().isBlank()) {
            throw new IllegalArgumentException("admin user: email is required");
        }
        String email = EmailAddresses.normalize(request.email());
        if (request.password() == null || request.password().isBlank()) {
            throw new IllegalArgumentException("admin user '" + email + "': password is required");
        }
        return new AdminUserConfig(email, hash(request.password()), displayName(request.displayName()), true, clock.instant());
    }

    /// The admin user `id` as a response body.
    AdminUserView toAdminUserView(String id, AdminUserConfig user) {
        return new AdminUserView(id, user.email(), user.displayName(), user.enabled(), user.createdAt());
    }

    private static HashedPassword hash(String plaintext) {
        return HashedPassword.fromPlaintext(Mechanism.PLAIN, plaintext);
    }

    /// A blank display name means none.
    private static @Nullable String displayName(@Nullable String requested) {
        return requested == null || requested.isBlank() ? null : requested;
    }
}
