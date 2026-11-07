package io.jonasg.kawa.plugin.auth;

import io.jonasg.kawa.plugin.http.HttpEndpoint;

/// What the gateway hands to [AuthenticationProviderFactory#create].
public interface AuthenticationProviderContext {

    /// The provider's settings: the `admin.auth.settings` section of the gateway config.
    ProviderSettings settings();

    /// kawa's local admin users, for providers that check usernames and passwords themselves.
    AdminUserDirectory adminUsers();

    /// The OAuth clients managed through the admin API, for providers that run an authorization server.
    /// Look clients up on every request instead of keeping them: see [OAuthClientDirectory].
    OAuthClientDirectory oauthClients();

    /// Whether the operator currently lets clients identify themselves with an OAuth Client ID Metadata Document
    /// (a `client_id` that is an `https` URL) instead of being registered through [#oauthClients]. Switched at
    /// runtime, so ask on every request instead of keeping the answer: once it turns `false`, a provider that
    /// runs an authorization server should refuse new sign-ins of such clients and stop accepting their tokens.
    boolean clientIdMetadataDocumentsEnabled();

    /// The absolute URL clients reach the admin API at, without a trailing `/`: `admin.publicUrl`
    /// from the gateway config, or `http://<bind host>:<admin port>` when that is not set. Use it for
    /// URLs a provider publishes, such as a token issuer or a metadata document.
    String publicUrl();

    /// Registers an admin HTTP endpoint that is reachable without authentication, e.g. a token
    /// endpoint or a key set. `method` is an HTTP method such as `GET`; `path` is an exact path such
    /// as `/oauth/token`.
    void publicEndpoint(String method, String path, HttpEndpoint endpoint);
}
