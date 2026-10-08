package io.jonasg.kawa.http.auth.oauth;

import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Clock;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;

/// `admin.auth.type: oauth`: kawa acts as an OAuth 2.0 authorization server for its own admin users.
///
/// Settings (`admin.auth.settings`):
///
/// - `issuer`: the `iss` and `aud` claim of issued tokens (defaults to `kawa`)
/// - `tokenTtl`: access token lifetime as an ISO-8601 duration (defaults to `PT15M`)
/// - `sessionTtl`: how long a login lasts before the refresh token stops working; refreshing does not
///   extend it (defaults to `PT8H`)
/// - `signingKey`: PEM EC P-256 private key; generated at startup when absent
/// - `loginUrl` (required): the external login UI (kawa studio's login page) that `/oauth/authorize`
///   redirects to with a `login_challenge`; kawa serves no HTML itself
/// - `redirectUris` (required): the redirect URIs the client may use, e.g. kawa studio's
///   `…/admin/auth/callback`
/// - `clientId`: the client id of the UI (defaults to `kawa-studio`)
///
/// Users log in with the authorization code flow with PKCE (no password grant, per OAuth 2.1).
///
/// Requires at least one admin user: without one nobody could log in to create further admin users.
/// The gateway seeds `admin.auth.bootstrapUser` as the first admin user before the provider is created.
public final class OAuthAuthenticationProviderFactory implements AuthenticationProviderFactory {

    static final String DEFAULT_ISSUER = "kawa";
    static final Duration DEFAULT_TOKEN_TTL = Duration.ofMinutes(15);
    static final Duration DEFAULT_SESSION_TTL = Duration.ofHours(8);
    static final String DEFAULT_CLIENT_ID = "kawa-studio";

    private final Clock clock;

    public OAuthAuthenticationProviderFactory() {
        this(Clock.systemUTC());
    }

    OAuthAuthenticationProviderFactory(Clock clock) {
        this.clock = clock;
    }

    @Override
    public String name() {
        return "oauth";
    }

    @Override
    public AuthenticationProvider create(AuthenticationProviderContext context) {
        if (!context.adminUsers().hasAdminUsers()) {
            throw new IllegalArgumentException("admin.auth.type 'oauth' requires an admin user: "
                    + "set admin.auth.bootstrapUser to create the first one");
        }
        var settings = context.settings();
        String issuer = settings.string("issuer").orElse(DEFAULT_ISSUER);
        Duration tokenTtl = positive("tokenTtl", settings.duration("tokenTtl").orElse(DEFAULT_TOKEN_TTL));
        Duration sessionTtl = positive("sessionTtl", settings.duration("sessionTtl").orElse(DEFAULT_SESSION_TTL));
        String loginUrl = absoluteUri("loginUrl", settings.requireString("loginUrl")).toString();
        String clientId = settings.string("clientId").orElse(DEFAULT_CLIENT_ID);
        Set<URI> redirectUris = settings.strings("redirectUris").stream()
                .map(uri -> absoluteUri("redirectUris", uri))
                .collect(Collectors.toUnmodifiableSet());
        if (redirectUris.isEmpty()) {
            throw new IllegalArgumentException("admin.auth.settings.redirectUris is required");
        }
        var keys = SigningKeys.from(settings.string("signingKey").orElse(null));
        var tokenIssuer = new TokenIssuer(keys, issuer, tokenTtl, sessionTtl, clock);
        var refreshTokens = new TokenVerifier(keys, issuer, TokenIssuer.REFRESH_TOKEN_TYPE);
        var flow = new AuthorizationFlow(keys, issuer, tokenIssuer, clock);

        context.publicEndpoint("GET", "/oauth/authorize", new AuthorizeEndpoint(flow, clientId, redirectUris, loginUrl));
        context.publicEndpoint("POST", "/oauth/login", new LoginEndpoint(flow, context.adminUsers()));
        context.publicEndpoint("POST", "/oauth/token", new TokenEndpoint(context.adminUsers(), tokenIssuer, refreshTokens, flow));
        context.publicEndpoint("GET", "/.well-known/jwks.json", new JwksEndpoint(keys));
        context.publicEndpoint("GET", "/.well-known/oauth-authorization-server", new AuthorizationServerMetadataEndpoint(issuer));
        return new KawaTokenAuthenticationProvider(keys, issuer, context.adminUsers());
    }

    private static URI absoluteUri(String setting, String value) {
        try {
            URI uri = new URI(value);
            if (uri.isAbsolute() && uri.getHost() != null) {
                return uri;
            }
        } catch (URISyntaxException e) {
            // reported below
        }
        throw new IllegalArgumentException("admin.auth.settings." + setting + " must be an absolute URL: " + value);
    }

    private static Duration positive(String setting, Duration value) {
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException("admin.auth.settings." + setting + " must be positive");
        }
        return value;
    }
}
