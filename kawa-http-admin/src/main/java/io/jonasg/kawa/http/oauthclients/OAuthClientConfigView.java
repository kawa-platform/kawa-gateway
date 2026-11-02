package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.config.OAuthClientConfig;
import org.jspecify.annotations.Nullable;

import java.util.List;

/// An OAuth client in an admin `/admin/oauth-clients` response.
///
/// @param id           the `client_id`
/// @param name         the human-readable label, `null` when none is set
/// @param enabled      whether the client may sign in
/// @param redirectUris the redirect URIs, as stored
/// @param audience     where the client's tokens are accepted: `mcp` or `admin`
record OAuthClientConfigView(
        String id,
        @Nullable String name,
        boolean enabled,
        List<String> redirectUris,
        OAuthClientConfig.Audience audience
) {
}
