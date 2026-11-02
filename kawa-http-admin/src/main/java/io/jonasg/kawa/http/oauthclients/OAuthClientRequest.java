package io.jonasg.kawa.http.oauthclients;

import org.jspecify.annotations.NullUnmarked;

import java.util.List;

/// An OAuth client in a `PUT /admin/oauth-clients/{id}` body. The id comes from the path. Validated and converted by
/// [OAuthClientConfigMapper].
///
/// @param name         an optional human-readable label
/// @param enabled      whether the client may sign in; defaults to `true`
/// @param redirectUris the redirect URIs; at least one
/// @param audience     `mcp` or `admin`; required
@NullUnmarked
record OAuthClientRequest(String name, Boolean enabled, List<String> redirectUris, String audience) {
}
