package io.jonasg.kawa.plugin.auth;

/// The authenticated caller of the admin HTTP API.
///
/// @param name the principal name, e.g. the admin username or the `sub` claim of a token
public record KawaPrincipal(String name) {

    public KawaPrincipal {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("principal name must not be null or blank");
        }
    }
}
