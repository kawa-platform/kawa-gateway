package io.jonasg.kawa.plugin.auth;

/// The authenticated caller of the admin HTTP API: `name` is who the request is for, `clientId` the
/// verified OAuth client that made it. kawa identifies clients by `clientId`, never by `name`.
///
/// - admin user through kawa studio: `("alice@acme.com", "kawa-studio")`
/// - operator for itself: `("kawa-operator", "kawa-operator")`
/// - operator for a delegated principal: `("k8s:team-a", "kawa-operator")`
///
/// @param name     the principal governance judges, e.g. an admin username or a token's `sub`
/// @param clientId the OAuth client the caller came through, e.g. `kawa-studio`, `kawa-mcp-claude` or
///                 `kawa-operator`; `null` when the provider has no such notion, as with API keys or no
///                 authentication. Providers must set it only for a client they verified
public record KawaPrincipal(String name, String clientId) {

    public KawaPrincipal {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("principal name must not be null or blank");
        }
        if (clientId != null && clientId.isBlank()) {
            throw new IllegalArgumentException("principal client id must be null or not blank");
        }
    }

    /// A principal that did not come through an OAuth client.
    public KawaPrincipal(String name) {
        this(name, null);
    }
}
