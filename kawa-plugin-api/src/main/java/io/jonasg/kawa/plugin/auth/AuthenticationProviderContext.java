package io.jonasg.kawa.plugin.auth;

import io.jonasg.kawa.plugin.http.HttpEndpoint;

/// What the gateway hands to [AuthenticationProviderFactory#create].
public interface AuthenticationProviderContext {

    /// The provider's settings: the `admin.auth.settings` section of the gateway config.
    ProviderSettings settings();

    /// kawa's local admin users, for providers that check usernames and passwords themselves.
    AdminUserDirectory adminUsers();

    /// Registers an admin HTTP endpoint that is reachable without authentication, e.g. a token
    /// endpoint or a key set. `method` is an HTTP method such as `GET`; `path` is an exact path such
    /// as `/oauth/token`.
    void publicEndpoint(String method, String path, HttpEndpoint endpoint);
}
