package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.config.OAuthClientConfig;
import io.jonasg.kawa.config.OAuthClientConfig.Audience;
import org.jspecify.annotations.Nullable;

/// Converts between the `/admin/oauth-clients` transport types and [OAuthClientConfig].
///
/// The mapper converts the audience strictly; the id, the redirect URIs and the defaults are checked by
/// [OAuthClientConfig] itself, whose messages name the client and the field at fault.
///
/// A stateless instance created once in [OAuthClientRoutes] and shared by the OAuth client handlers.
final class OAuthClientConfigMapper {

    /// An OAuth client request as the config record. The id comes from the path; a body never decides it.
    ///
    /// @throws IllegalArgumentException when the body is missing or any field is invalid
    OAuthClientConfig toOAuthClientConfig(String id, @Nullable OAuthClientRequest req) {
        if (req == null) {
            throw new IllegalArgumentException("OAuth client '" + id + "': body is required");
        }
        return new OAuthClientConfig(id, req.name(), req.enabled(), req.redirectUris(), toAudience(id, req.audience()));
    }

    /// A single OAuth client as the response body.
    OAuthClientConfigView toOAuthClientConfigView(OAuthClientConfig config) {
        return new OAuthClientConfigView(config.id(), config.name(), config.enabled(), config.redirectUris(), config.audience());
    }

    private static @Nullable Audience toAudience(String id, @Nullable String audience) {
        if (audience == null) {
            return null;
        }
        return switch (audience) {
            case "mcp" -> Audience.MCP;
            case "admin" -> Audience.ADMIN;
            default -> throw new IllegalArgumentException(
                    "OAuth client '" + id + "': unsupported audience '" + audience + "', expected 'mcp' or 'admin'");
        };
    }
}
