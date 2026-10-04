package io.jonasg.kawa.config;

import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/// Authentication for the admin HTTP API. Bootstrap-only (YAML): it carries secrets and is never
/// written to the config topic.
///
/// The authentication itself is done by a pluggable provider, selected by name. kawa ships `none`
/// (the admin API is open) and `oauth` (kawa acts as an OAuth authorization server); plugins can
/// register more, e.g. `oidc`.
///
/// @param type          the name of the authentication provider (defaults to `none`)
/// @param bootstrapUser an in-memory admin user that can always log in to providers that check
///                      kawa's local admin users
/// @param settings      provider-specific settings handed to the selected provider as-is, with
///                      `${VAR:-default}` placeholders in string values resolved
public record AdminAuthConfig(
        String type,
        @Nullable BootstrapUser bootstrapUser,
        Map<String, Object> settings
) {

    /// The provider used when `admin.auth.type` is not set.
    public static final String DEFAULT_TYPE = "none";

    public AdminAuthConfig {
        if (type == null || type.isBlank()) {
            type = DEFAULT_TYPE;
        }
        settings = settings == null ? Map.of() : resolveEnvVars(settings, System::getenv);
    }

    /// The default: no admin authentication.
    public static AdminAuthConfig none() {
        return new AdminAuthConfig(DEFAULT_TYPE, null, null);
    }

    static Map<String, Object> resolveEnvVars(Map<String, Object> settings, Function<String, String> envLookup) {
        var resolved = new HashMap<String, Object>();
        settings.forEach((key, value) -> {
            if (value != null) {
                resolved.put(key, resolveValue(value, envLookup));
            }
        });
        return Map.copyOf(resolved);
    }

    private static Object resolveValue(Object value, Function<String, String> envLookup) {
        return switch (value) {
            case String text -> ClientConfig.resolveEnvVars(text, envLookup);
            case List<?> list -> list.stream().map(item -> resolveValue(item, envLookup)).toList();
            default -> value;
        };
    }

    /// An admin user defined in the bootstrap config. Its password is plaintext (typically
    /// `${KAWA_ADMIN_PASSWORD}`), resolved from the environment, and only ever held in memory.
    ///
    /// @param username the admin username
    /// @param password the plaintext password, with `${VAR:-default}` resolved
    public record BootstrapUser(String username, String password) {

        public BootstrapUser {
            if (username == null || username.isBlank()) {
                throw new IllegalArgumentException("admin.auth.bootstrapUser.username must not be null or blank");
            }
            if (password == null || password.isBlank()) {
                throw new IllegalArgumentException("admin.auth.bootstrapUser.password must not be null or blank");
            }
            password = ClientConfig.resolveEnvVars(password, System::getenv);
        }

        static BootstrapUser of(String username, String password, Function<String, String> envLookup) {
            if (password == null || password.isBlank()) {
                throw new IllegalArgumentException("admin.auth.bootstrapUser.password must not be null or blank");
            }
            return new BootstrapUser(username, ClientConfig.resolveEnvVars(password, envLookup));
        }
    }
}
