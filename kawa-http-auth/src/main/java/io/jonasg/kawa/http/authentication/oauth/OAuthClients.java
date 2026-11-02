package io.jonasg.kawa.http.authentication.oauth;

import com.nimbusds.jwt.JWTClaimsSet;
import io.jonasg.kawa.plugin.auth.OAuthClient;
import io.jonasg.kawa.plugin.auth.OAuthClientDirectory;
import org.jspecify.annotations.Nullable;

import java.net.URI;
import java.text.ParseException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/// The OAuth clients the authorization server issues tokens to: kawa studio, configured in the provider's
/// settings, and the clients managed through the admin API.
///
/// kawa studio is always resolved first, so a managed client with the same id is never used. Managed clients are
/// looked up on every call, so a change through the admin API applies to the next request; a disabled client is
/// treated as unknown. The same holds for the tokens already issued ([#clientOf]): disabling or deleting a client,
/// or changing its audience, revokes them.
final class OAuthClients {

    /// A client the authorization server knows.
    ///
    /// @param id           the `client_id`
    /// @param audience     the `aud` of the client's tokens
    /// @param redirectUris where the client may be redirected to
    record Client(String id, String audience, List<RegisteredRedirectUri> redirectUris) {

        /// Whether the client may be redirected to `requested`.
        boolean allowsRedirect(@Nullable URI requested) {
            return requested != null && redirectUris.stream().anyMatch(uri -> uri.allows(requested));
        }
    }

    private final Client studio;
    private final OAuthClientDirectory managed;
    private final String adminAudience;
    private final String mcpAudience;

    /// @param adminAudience the `aud` of tokens valid on the whole admin API: the issuer
    /// @param mcpAudience   the `aud` of tokens for the MCP endpoint
    OAuthClients(String studioId, Set<URI> studioRedirectUris, OAuthClientDirectory managed, String adminAudience, String mcpAudience) {
        this.studio = new Client(studioId, adminAudience, studioRedirectUris.stream().map(RegisteredRedirectUri::exact).toList());
        this.managed = managed;
        this.adminAudience = adminAudience;
        this.mcpAudience = mcpAudience;
    }

    /// The client with this id, unless it is unknown or disabled.
    Optional<Client> find(@Nullable String id) {
        if (id == null) {
            return Optional.empty();
        }
        if (studio.id().equals(id)) {
            return Optional.of(studio);
        }
        return managed.find(id).filter(OAuthClient::enabled).map(this::client);
    }

    /// The client a session token (access or refresh token) was issued to, while its tokens are still valid: the
    /// client named by the token's `client_id` (kawa studio for a token issued before tokens named their client) is
    /// known and enabled, and its tokens still carry the token's `aud`. Empty once the client was disabled, deleted
    /// or given another audience.
    Optional<Client> clientOf(JWTClaimsSet token) {
        String clientId;
        try {
            clientId = token.getStringClaim(TokenIssuer.CLIENT_ID_CLAIM);
        } catch (ParseException e) {
            return Optional.empty();
        }
        return find(clientId == null ? studio.id() : clientId)
                .filter(client -> token.getAudience().equals(List.of(client.audience())));
    }

    /// The `aud` of tokens for the MCP endpoint, which are accepted only on `POST /mcp`.
    String mcpAudience() {
        return mcpAudience;
    }

    /// Whether the RFC 8707 `resources` a client requested tokens for are acceptable: none, or only `audience`, the
    /// `aud` of the client's tokens. kawa studio's requests are not checked, as they never were.
    boolean acceptsResources(String clientId, String audience, @Nullable List<URI> resources) {
        return studio.id().equals(clientId) || resources == null
                || resources.stream().allMatch(resource -> resource.toString().equals(audience));
    }

    private Client client(OAuthClient client) {
        String audience = switch (client.audience()) {
            case ADMIN -> adminAudience;
            case MCP -> mcpAudience;
        };
        return new Client(client.id(), audience,
                client.redirectUris().stream().flatMap(uri -> RegisteredRedirectUri.parse(uri).stream()).toList());
    }
}
