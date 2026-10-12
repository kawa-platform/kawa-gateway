package io.jonasg.kawa.plugin.auth;

/// The authenticated caller of the admin HTTP API.
///
/// @param name  the principal name, e.g. the admin username or the `sub` claim of a token. Governance
///              judges the request as this principal
/// @param actor who made the request on the principal's behalf (a delegating service client such as
///              the kawa operator, the `act` claim of a token), or `null` when the principal acts itself
public record KawaPrincipal(String name, String actor) {

    public KawaPrincipal {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("principal name must not be null or blank");
        }
        if (actor != null && actor.isBlank()) {
            throw new IllegalArgumentException("principal actor must be null or not blank");
        }
    }

    /// A principal acting for itself.
    public KawaPrincipal(String name) {
        this(name, null);
    }
}
