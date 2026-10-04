package io.jonasg.kawa.plugin.auth;

import java.util.Optional;

/// Authenticates a request to the admin HTTP API.
///
/// The gateway calls this for every request to a non-public admin endpoint, off the network event
/// loop. Returning a principal admits the request; returning empty answers `401 Unauthorized`.
/// Implementations must be thread-safe and must not throw for bad credentials: a missing, malformed
/// or expired credential is simply an empty result.
///
/// Kawa ships an implementation that verifies its own OAuth access tokens. Plugins can provide
/// others, for example one that verifies tokens issued by an external OpenID Connect provider.
@FunctionalInterface
public interface AuthenticationProvider {

    Optional<KawaPrincipal> authenticate(AuthenticationRequest request);
}
