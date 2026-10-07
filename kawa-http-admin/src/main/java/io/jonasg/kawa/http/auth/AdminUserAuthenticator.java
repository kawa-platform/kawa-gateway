package io.jonasg.kawa.http.auth;

import io.jonasg.kawa.config.AdminAuthConfig.BootstrapUser;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.Mechanism;
import io.jonasg.kawa.plugin.auth.AdminUserDirectory;
import org.jspecify.annotations.Nullable;

import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

/// kawa's [AdminUserDirectory]: checks admin usernames and passwords against the bootstrap user from
/// the YAML config first, then the admin users stored in the config topic. Usernames are email
/// addresses and match regardless of case.
///
/// A stored admin user's subject is their id, the bootstrap user's subject their username. A
/// disabled admin user counts as unknown: they cannot log in, and since every token check asks
/// [#username], the tokens already issued to them stop working immediately, as for a deleted user.
///
/// Every check costs one PBKDF2 verification, also for unknown users (against a dummy hash), so the
/// response time does not reveal which usernames exist. The bootstrap password is hashed once at
/// startup for the same reason.
public final class AdminUserAuthenticator implements AdminUserDirectory {

    private final @Nullable String bootstrapUsername;
    private final @Nullable HashedPassword bootstrapPassword;
    private final GatewayConfigRepository repository;
    private final HashedPassword dummy = HashedPassword.fromPlaintext(Mechanism.PLAIN, UUID.randomUUID().toString());

    public AdminUserAuthenticator(@Nullable BootstrapUser bootstrapUser, GatewayConfigRepository repository) {
        this.bootstrapUsername = bootstrapUser == null ? null : bootstrapUser.username();
        this.bootstrapPassword = bootstrapUser == null ? null : HashedPassword.fromPlaintext(Mechanism.PLAIN, bootstrapUser.password());
        this.repository = repository;
    }

    @Override
    public Optional<String> authenticate(String username, String password) {
        Account account = find(username);
        boolean matches = (account == null ? dummy : account.password()).verify(password);
        return account != null && matches ? Optional.of(account.subject()) : Optional.empty();
    }

    @Override
    public Optional<String> username(String subject) {
        if (subject.equals(bootstrapUsername)) {
            return Optional.of(bootstrapUsername);
        }
        var user = repository.getActiveConfigOrEmpty().adminUsers().get(subject);
        return user == null || !user.enabled() ? Optional.empty() : Optional.of(user.email());
    }

    @Override
    public Optional<String> bootstrapUsername() {
        return Optional.ofNullable(bootstrapUsername);
    }

    /// Whether `email` is the bootstrap user's username, regardless of case.
    public boolean isBootstrapUser(String email) {
        return bootstrapUsername != null && bootstrapUsername.equalsIgnoreCase(email.trim());
    }

    private @Nullable Account find(String username) {
        if (isBootstrapUser(username) && bootstrapPassword != null) {
            return new Account(bootstrapUsername, bootstrapPassword);
        }
        String email = username.trim().toLowerCase(Locale.ROOT);
        return repository.getActiveConfigOrEmpty().adminUsers().entrySet().stream()
                .filter(entry -> entry.getValue().enabled() && entry.getValue().email().equals(email))
                .map(entry -> new Account(entry.getKey(), entry.getValue().password()))
                .findFirst()
                .orElse(null);
    }

    /// An admin user who may log in: the subject their tokens carry and their password hash.
    private record Account(String subject, HashedPassword password) {
    }
}
