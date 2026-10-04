package io.jonasg.kawa.plugin.auth;

/// Creates an [AuthenticationProvider] for the admin HTTP API.
///
/// Factories are registered by plugins through
/// [io.jonasg.kawa.plugin.KawaPluginContext#registerAuthenticationProvider] and selected by [#name]
/// with `admin.auth.type` in the gateway config. Only the selected factory is created, once, at
/// startup. Kawa's own `none` and `oauth` providers are registered the same way.
public interface AuthenticationProviderFactory {

    /// The value of `admin.auth.type` that selects this provider, e.g. `oauth` or `oidc`.
    String name();

    /// Creates the provider. Read settings, register public endpoints (such as a token endpoint) and
    /// validate everything here: throwing [IllegalArgumentException] for a bad setting aborts startup.
    AuthenticationProvider create(AuthenticationProviderContext context);
}
