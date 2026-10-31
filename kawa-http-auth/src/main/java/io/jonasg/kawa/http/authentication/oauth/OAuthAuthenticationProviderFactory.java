package io.jonasg.kawa.http.authentication.oauth;

import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory;

import java.net.InetAddress;
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
/// - `issuer`: the `iss` and `aud` claim of issued tokens, also published as the authorization
///   server's issuer (defaults to the admin API's public URL, `admin.publicUrl`)
/// - `tokenTtl`: access token lifetime as an ISO-8601 duration (defaults to `PT15M`)
/// - `sessionTtl`: how long a login lasts before the refresh token stops working; refreshing does not
///   extend it (defaults to `PT8H`)
/// - `signingKey`: PEM EC P-256 private key; generated at startup when absent
/// - `loginUrl` (required): the external login UI (kawa studio's login page) that `/oauth/authorize`
///   redirects to with a `login_challenge`; kawa serves no HTML itself
/// - `redirectUris` (required): the redirect URIs the client may use, e.g. kawa studio's
///   `…/admin/auth/callback`
/// - `operatorSecret`: the secret of the kawa operator (client id `kawa-operator`), which obtains tokens
///   with the `client_credentials` grant; the operator is disabled when absent
/// - `operatorActsAs`: the principals the operator may act as through token exchange (RFC 8693), an exact
///   principal or a prefix ending in `*` with a `:` in it (defaults to `k8s:*`)
///
/// The UI is always the client `kawa-studio`.
///
/// Users log in with the authorization code flow with PKCE (no password grant, per OAuth 2.1).
///
/// Requires a public URL (`admin.publicUrl`, or the bind address it defaults to) that clients can reach:
/// a wildcard bind address such as `0.0.0.0` is refused, as tokens and metadata would name it.
///
/// Requires at least one admin user: without one nobody could log in to create further admin users.
/// The gateway seeds `admin.auth.bootstrapUser` as the first admin user before the provider is created.
public final class OAuthAuthenticationProviderFactory implements AuthenticationProviderFactory {

    static final Duration DEFAULT_TOKEN_TTL = Duration.ofMinutes(15);
    static final Duration DEFAULT_SESSION_TTL = Duration.ofHours(8);

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
        requireReachable(context.publicUrl());
        var settings = context.settings();
        String issuer = settings.string("issuer").orElseGet(context::publicUrl);
        Duration tokenTtl = positive("tokenTtl", settings.duration("tokenTtl").orElse(DEFAULT_TOKEN_TTL));
        Duration sessionTtl = positive("sessionTtl", settings.duration("sessionTtl").orElse(DEFAULT_SESSION_TTL));
        String loginUrl = absoluteUri("loginUrl", settings.requireString("loginUrl")).toString();
        Set<URI> redirectUris = settings.strings("redirectUris").stream()
                .map(uri -> absoluteUri("redirectUris", uri))
                .collect(Collectors.toUnmodifiableSet());
        if (redirectUris.isEmpty()) {
            throw new IllegalArgumentException("admin.auth.settings.redirectUris is required");
        }
        var operator = OperatorClient.from(settings.string("operatorSecret").orElse(null),
                settings.string("operatorActsAs").orElse(OperatorClient.DEFAULT_ACTS_AS));
        var keys = SigningKeys.from(settings.string("signingKey").orElse(null));
        var tokenIssuer = new TokenIssuer(keys, issuer, tokenTtl, sessionTtl, clock);
        var refreshTokens = new TokenVerifier(keys, issuer, TokenIssuer.REFRESH_TOKEN_TYPE);
        var flow = new AuthorizationFlow(keys, issuer, tokenIssuer, clock);

        context.publicEndpoint("GET", "/oauth/authorize", new AuthorizeEndpoint(flow, TokenIssuer.STUDIO_CLIENT_ID, redirectUris, loginUrl));
        context.publicEndpoint("POST", "/oauth/login", new LoginEndpoint(flow, context.adminUsers()));
        context.publicEndpoint("POST", "/oauth/token", new TokenEndpoint(context.adminUsers(), tokenIssuer, refreshTokens, flow, operator,
                new TokenVerifier(keys, issuer, TokenIssuer.ACCESS_TOKEN_TYPE)));
        context.publicEndpoint("GET", "/.well-known/jwks.json", new JwksEndpoint(keys));
        context.publicEndpoint("GET", "/.well-known/oauth-authorization-server", new AuthorizationServerMetadataEndpoint(issuer));
        context.publicEndpoint("GET", ProtectedResourceMetadataEndpoint.PATH, new ProtectedResourceMetadataEndpoint(context.publicUrl(), issuer));
        return new KawaTokenAuthenticationProvider(keys, issuer, context.adminUsers(), operator, context.publicUrl());
    }

    private static void requireReachable(String publicUrl) {
        String host = URI.create(publicUrl).getHost();
        if (host != null && host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        if (host != null && isWildcard(host)) {
            throw new IllegalArgumentException("admin.auth.type 'oauth' needs the URL clients reach the admin API at, but the "
                    + "public URL " + publicUrl + " is a wildcard bind address: set admin.publicUrl, e.g. https://kawa.example.com");
        }
    }

    private static boolean isWildcard(String host) {
        try {
            return InetAddress.ofLiteral(host).isAnyLocalAddress();
        } catch (IllegalArgumentException notAnIpLiteral) {
            return false;
        }
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
