package io.jonasg.kawa.http.auth;

import io.jonasg.kawa.config.AdminAuthConfig.BootstrapUser;
import io.jonasg.kawa.config.AdminUserConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.plugin.auth.AdminUserDirectory;
import org.jspecify.annotations.Nullable;

import java.util.Optional;
import java.util.UUID;

/// kawa's [AdminUserDirectory]: checks admin usernames and passwords against the bootstrap user from
/// the YAML config first, then the admin users stored in the config topic.
///
/// Every check costs one PBKDF2 verification, also for unknown users (against a dummy hash), so the
/// response time does not reveal which usernames exist. The bootstrap password is hashed once at
/// startup for the same reason.
public final class AdminUserAuthenticator implements AdminUserDirectory {

    private final @Nullable String bootstrapUsername;
    private final @Nullable AdminUserConfig bootstrapUser;
    private final GatewayConfigRepository repository;
    private final AdminUserConfig dummy = AdminUserConfig.fromPlaintext(UUID.randomUUID().toString());

    public AdminUserAuthenticator(@Nullable BootstrapUser bootstrapUser, GatewayConfigRepository repository) {
        this.bootstrapUsername = bootstrapUser == null ? null : bootstrapUser.username();
        this.bootstrapUser = bootstrapUser == null ? null : AdminUserConfig.fromPlaintext(bootstrapUser.password());
        this.repository = repository;
    }

    @Override
    public boolean verify(String username, String password) {
        AdminUserConfig user = find(username);
        boolean matches = (user == null ? dummy : user).password().verify(password);
        return user != null && matches;
    }

    @Override
    public boolean exists(String username) {
        return find(username) != null;
    }

    @Override
    public Optional<String> bootstrapUsername() {
        return Optional.ofNullable(bootstrapUsername);
    }

    private @Nullable AdminUserConfig find(String username) {
        if (username.equals(bootstrapUsername)) {
            return bootstrapUser;
        }
        return repository.getActiveConfigOrEmpty().adminUsers().get(username);
    }
}
