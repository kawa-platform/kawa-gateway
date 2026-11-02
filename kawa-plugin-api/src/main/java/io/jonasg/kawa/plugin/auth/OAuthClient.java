package io.jonasg.kawa.plugin.auth;

import java.util.List;

/// An OAuth client managed through kawa's admin API (`/admin/oauth-clients`), such as an MCP client: a public
/// client using PKCE, without a secret. The client a provider configures itself, such as kawa studio, is not one
/// of these.
///
/// @param id           the `client_id`
/// @param enabled      whether the client may sign in; what a disabled client may still do is up to the provider
/// @param redirectUris the redirect URIs exactly as registered. An `http` loopback URI may give `*` as its entire
///                     port, e.g. `http://localhost:*/callback`, meaning any port with the same host and path
/// @param audience     where the client's tokens are meant to be accepted
public record OAuthClient(String id, boolean enabled, List<String> redirectUris, Audience audience) {

    /// Where an OAuth client's tokens are meant to be accepted.
    public enum Audience {
        /// Only on `POST /mcp`.
        MCP,
        /// On the whole admin API, `/mcp` included.
        ADMIN
    }

    public OAuthClient {
        redirectUris = List.copyOf(redirectUris);
    }
}
