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
/// gateway-assigned UUID; their email must be unique among them. Every check runs inside the config
/// update, on the snapshot that is written.
///
/// The stored admin users are the only way in to the admin API, so this service refuses the changes
/// that would lock everyone out: removing or disabling the caller's own admin user, and removing or
/// disabling the last enabled one. The checks run inside the mutation, on the snapshot that is written:
/// writes here are serialized, and the gateway's config repository commits each mutation as a
/// compare-and-set across replicas (re-running it on a newer snapshot when another replica wrote in
/// between), so two concurrent requests cannot each remove "not the last" enabled admin user.
final class AdminUserService {

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;
    private final Object writeLock = new Object();

    AdminUserService(GatewayConfigRepository repository) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
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
        synchronized (writeLock) {
            updater.update(consistency, config -> {
                requireUnusedEmail(config, user.email(), id);
                return config.upsertAdminUser(id, user);
            });
        }
        return id;
    }

    /// Changes the fields of the admin user `id` that `patch` carries, keeps the rest, and returns the
    /// stored result.
    ///
    /// @param caller the principal name of the caller, which kawa's `oauth` provider sets to the admin
    ///               user's email; `null` when there is none
    AdminUserConfig updateAdminUser(String id, UpdateAdminUserRequest patch, @Nullable String caller, Consistency consistency) {
        synchronized (writeLock) {
            updater.update(consistency, config -> config.upsertAdminUser(id, patched(config, id, patch, caller)));
        }
        return getAdminUser(id);
    }

    /// @param caller the principal name of the caller, which kawa's `oauth` provider sets to the admin
    ///               user's email; `null` when there is none
    void deleteAdminUser(String id, @Nullable String caller, Consistency consistency) {
        synchronized (writeLock) {
            updater.update(consistency, config -> {
                AdminUserConfig existing = config.adminUsers().get(id);
                if (existing == null) {
                    throw notFound(id);
                }
                if (isCaller(existing, caller)) {
                    throw new ConflictException("you cannot delete your own admin user '" + existing.email()
                            + "'; sign in as another admin user to delete it");
                }
                requireAnotherEnabledUser(config, id, existing, "delete");
                return config.removeAdminUser(id);
            });
        }
    }

    /// The admin user `id` in `config` with `patch` applied: fields it leaves out keep their value, an
    /// empty display name clears it, and `createdAt` never changes.
    private AdminUserConfig patched(GatewayConfig config, String id, UpdateAdminUserRequest patch, @Nullable String caller) {
        AdminUserConfig existing = config.adminUsers().get(id);
        if (existing == null) {
            throw notFound(id);
        }
        if (patch.password() != null && patch.password().isBlank()) {
            throw new IllegalArgumentException("admin user '" + existing.email() + "': password must not be blank");
        }
        if (Boolean.FALSE.equals(patch.enabled())) {
            if (isCaller(existing, caller)) {
                throw new ConflictException("you cannot disable your own admin user '" + existing.email()
                        + "'; sign in as another admin user to disable it");
            }
            requireAnotherEnabledUser(config, id, existing, "disable");
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

    /// Rejects deleting or disabling `existing` when it is the last enabled admin user: nobody could
    /// log in afterwards. A user that is already disabled can always go.
    private static void requireAnotherEnabledUser(GatewayConfig config, String id, AdminUserConfig existing, String action) {
        if (!existing.enabled()) {
            return;
        }
        boolean anotherEnabled = config.adminUsers().entrySet().stream()
                .anyMatch(entry -> !entry.getKey().equals(id) && entry.getValue().enabled());
        if (!anotherEnabled) {
            throw new ConflictException("admin user '" + existing.email() + "' is the last enabled admin user; "
                    + "create or enable another admin user before you " + action + " it");
        }
    }

    /// Whether `user` is the caller. Matched on the email, the name kawa's `oauth` provider gives its
    /// principal; a provider naming principals differently never matches, and only the last-enabled
    /// check applies.
    private static boolean isCaller(AdminUserConfig user, @Nullable String caller) {
        return caller != null && user.email().equalsIgnoreCase(caller.trim());
    }

    private static @Nullable String blankToNull(String value) {
        return value.isBlank() ? null : value;
    }

    /// Rejects `email` when another admin user than `id` already has it.
    private static void requireUnusedEmail(GatewayConfig config, String email, String id) {
        boolean taken = config.adminUsers().entrySet().stream()
                .anyMatch(entry -> !entry.getKey().equals(id) && entry.getValue().email().equals(email));
        if (taken) {
            throw new ConflictException("admin user email '" + email + "' is already in use");
        }
    }

    private static NotFoundException notFound(String id) {
        return new NotFoundException("admin user '" + id + "' not found");
    }
}
