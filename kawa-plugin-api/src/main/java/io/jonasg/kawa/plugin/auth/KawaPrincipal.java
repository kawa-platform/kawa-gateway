package io.jonasg.kawa.plugin.auth;

/// The authenticated caller of the admin HTTP API: `name` is who the request is for, `actor` the
/// verified service client that made it. kawa identifies clients by `actor`, never by `name`.
///
/// - admin user: `("alice@acme.com", null)`
/// - operator for itself: `("kawa-operator", "kawa-operator")`
/// - operator for a delegated principal: `("k8s:team-a", "kawa-operator")`
///
/// @param name  the principal governance judges, e.g. an admin username or a token's `sub`
/// @param actor the client that made the request, or `null`; providers must set it only for a
///              client they verified
public record KawaPrincipal(String name, String actor) {

    public KawaPrincipal {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("principal name must not be null or blank");
        }
        if (actor != null && actor.isBlank()) {
            throw new IllegalArgumentException("principal actor must be null or not blank");
        }
    }

    /// A principal without an actor, e.g. an admin user.
    public KawaPrincipal(String name) {
        this(name, null);
    }
}
