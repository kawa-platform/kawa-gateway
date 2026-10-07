package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminUserConfig;
import io.jonasg.kawa.config.EmailAddresses;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.Mechanism;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.UUID;

/// Owns the admin users section of the gateway config snapshot. Admin users are keyed by a
/// gateway-assigned UUID; their email must be unique among them and must not be the bootstrap
/// user's. Every check runs inside the config update, on the snapshot that is written.
final class AdminUserService {

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;
    private final @Nullable String bootstrapUsername;

    /// @param bootstrapUsername the bootstrap admin from the YAML config, whose email no stored admin user may take
    AdminUserService(GatewayConfigRepository repository, @Nullable String bootstrapUsername) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
        this.bootstrapUsername = bootstrapUsername;
    }

    Map<String, AdminUserConfig> listAdminUsers() {
        return repository.getActiveConfigOrEmpty().adminUsers();
    }

    AdminUserConfig getAdminUser(String id) {
        AdminUserConfig user = listAdminUsers().get(id);
        if (user == null) {
            throw notFound(id);
        }
        return user;
    }

    /// Stores `user` under a new id and returns that id.
    String createAdminUser(AdminUserConfig user, Consistency consistency) {
        String id = UUID.randomUUID().toString();
        updater.update(consistency, config -> {
            requireUnusedEmail(config, user.email(), id);
            return config.upsertAdminUser(id, user);
        });
        return id;
    }

    /// Changes the fields of the admin user `id` that `patch` carries, keeps the rest, and returns the
    /// stored result.
    AdminUserConfig updateAdminUser(String id, UpdateAdminUserRequest patch, Consistency consistency) {
        updater.update(consistency, config -> config.upsertAdminUser(id, patched(config, id, patch)));
        return getAdminUser(id);
    }

    void deleteAdminUser(String id, Consistency consistency) {
        updater.update(consistency, config -> {
            if (!config.adminUsers().containsKey(id)) {
                throw notFound(id);
            }
            return config.removeAdminUser(id);
        });
    }

    /// The admin user `id` in `config` with `patch` applied: fields it leaves out keep their value, an
    /// empty display name clears it, and `createdAt` never changes.
    private AdminUserConfig patched(GatewayConfig config, String id, UpdateAdminUserRequest patch) {
        AdminUserConfig existing = config.adminUsers().get(id);
        if (existing == null) {
            throw notFound(id);
        }
        if (patch.password() != null && patch.password().isBlank()) {
            throw new IllegalArgumentException("admin user '" + existing.email() + "': password must not be blank");
        }
        var user = new AdminUserConfig(
                patch.email() == null ? existing.email() : EmailAddresses.normalize(patch.email()),
                patch.password() == null ? existing.password() : HashedPassword.fromPlaintext(Mechanism.PLAIN, patch.password()),
                patch.displayName() == null ? existing.displayName() : blankToNull(patch.displayName()),
                patch.enabled() == null ? existing.enabled() : patch.enabled(),
                existing.createdAt());
        requireUnusedEmail(config, user.email(), id);
        return user;
    }

    private static @Nullable String blankToNull(String value) {
        return value.isBlank() ? null : value;
    }

    /// Rejects `email` when the bootstrap user or another admin user than `id` already has it.
    private void requireUnusedEmail(GatewayConfig config, String email, String id) {
        boolean bootstrap = email.equalsIgnoreCase(bootstrapUsername);
        boolean taken = config.adminUsers().entrySet().stream()
                .anyMatch(entry -> !entry.getKey().equals(id) && entry.getValue().email().equals(email));
        if (bootstrap || taken) {
            throw new ConflictException("admin user email '" + email + "' is already in use");
        }
    }

    private static NotFoundException notFound(String id) {
        return new NotFoundException("admin user '" + id + "' not found");
    }
}
