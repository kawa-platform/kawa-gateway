package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminAuthConfig.BootstrapUser;
import io.jonasg.kawa.config.AdminUserConfig;
import io.jonasg.kawa.config.EmailAddresses;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.Mechanism;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Map;
import java.util.UUID;

/// Turns `admin.auth.bootstrapUser` into a stored admin user. On a gateway without admin users it
/// creates the bootstrap user as an ordinary, enabled admin user; once any admin user exists the
/// bootstrap user is ignored, so its configured password only counts on first start.
///
/// With the reset switch (`KAWA_ADMIN_RESET=true`) a start recovers a locked-out gateway: it
/// re-enables the admin user with the bootstrap email and resets its password, or creates it when
/// it is gone.
///
/// Runs once at startup, after the config topic has been read, so an unread topic is never mistaken
/// for an empty one. The bootstrap user's id is derived from its email, so gateway instances that
/// start together on an empty topic all write the same admin user and converge on one.
final class BootstrapUserSeeder {

    /// The environment variable that turns on the reset for one start.
    static final String RESET_ENV = "KAWA_ADMIN_RESET";

    private static final Logger log = LoggerFactory.getLogger(BootstrapUserSeeder.class);

    private final GatewayConfigRepository repository;
    private final Clock clock;

    BootstrapUserSeeder(GatewayConfigRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    /// Seeds `bootstrapUser`, or resets it when `reset` is set. Does nothing without a bootstrap user.
    void seed(@Nullable BootstrapUser bootstrapUser, boolean reset) {
        if (bootstrapUser == null) {
            if (reset) {
                log.warn("{} is set but admin.auth.bootstrapUser is not configured; no admin user was reset", RESET_ENV);
            }
            return;
        }
        String email = EmailAddresses.normalize(bootstrapUser.username());
        if (reset) {
            repository.updateAndWaitUntilApplied(config -> reset(config, email, bootstrapUser.password()));
            log.warn("{} is set: admin user '{}' was enabled and its password reset to admin.auth.bootstrapUser's. "
                    + "Unset {} before the next start.", RESET_ENV, email, RESET_ENV);
            return;
        }
        if (!repository.getActiveConfigOrEmpty().adminUsers().isEmpty()) {
            log.info("admin users exist; admin.auth.bootstrapUser '{}' is ignored", email);
            return;
        }
        // Checked again inside the update, on the snapshot that is written.
        repository.updateAndWaitUntilApplied(config -> config.adminUsers().isEmpty()
                ? config.upsertAdminUser(idOf(email), newUser(email, bootstrapUser.password()))
                : config);
        log.info("created admin user '{}' from admin.auth.bootstrapUser", email);
    }

    /// `config` with the admin user `email` enabled and given `password`; created if absent.
    private GatewayConfig reset(GatewayConfig config, String email, String password) {
        Map.Entry<String, AdminUserConfig> existing = config.adminUsers().entrySet().stream()
                .filter(entry -> entry.getValue().email().equals(email))
                .findFirst()
                .orElse(null);
        if (existing == null) {
            return config.upsertAdminUser(idOf(email), newUser(email, password));
        }
        AdminUserConfig user = existing.getValue();
        return config.upsertAdminUser(existing.getKey(),
                new AdminUserConfig(user.email(), hash(password), user.displayName(), true, user.createdAt()));
    }

    private AdminUserConfig newUser(String email, String password) {
        return new AdminUserConfig(email, hash(password), null, true, clock.instant());
    }

    /// The bootstrap user's id: a name-based UUID of its email, the same on every gateway instance.
    static String idOf(String email) {
        return UUID.nameUUIDFromBytes(("kawa-admin-user:" + email).getBytes(StandardCharsets.UTF_8)).toString();
    }

    private static HashedPassword hash(String plaintext) {
        return HashedPassword.fromPlaintext(Mechanism.PLAIN, plaintext);
    }
}
