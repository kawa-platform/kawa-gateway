package io.jonasg.kawa.plugin.auth;

import io.jonasg.kawa.plugin.http.HttpEndpoint;

/// What the gateway hands to [AuthenticationProviderFactory#create].
public interface AuthenticationProviderContext {

    /// The provider's settings: the `admin.auth.settings` section of the gateway config.
    ProviderSettings settings();

    /// kawa's local admin users, for providers that check usernames and passwords themselves.
    AdminUserDirectory adminUsers();

    /// The absolute URL clients reach the admin API at, without a trailing `/`: `admin.publicUrl`
    /// from the gateway config, or `http://<bind host>:<admin port>` when that is not set. Use it for
    /// URLs a provider publishes, such as a token issuer or a metadata document.
    String publicUrl();

    /// Registers an admin HTTP endpoint that is reachable without authentication, e.g. a token
    /// endpoint or a key set. `method` is an HTTP method such as `GET`; `path` is an exact path such
    /// as `/oauth/token`.
    void publicEndpoint(String method, String path, HttpEndpoint endpoint);
}
