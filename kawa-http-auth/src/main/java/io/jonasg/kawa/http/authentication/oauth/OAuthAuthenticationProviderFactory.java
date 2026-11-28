package io.jonasg.kawa.http.authentication.oauth;

import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/// `admin.auth.type: oauth`: kawa acts as an OAuth 2.0 authorization server for its own admin users.
///
/// Settings (`admin.auth.settings`):
///
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
/// - `clientIdMetadataDocuments`: an optional restriction on the clients that identify themselves with an OAuth
///   Client ID Metadata Document: when set, only these exact `client_id` URLs (each `https` with a path) are accepted;
///   when empty (the default), any `https` URL is. Whether such clients are accepted at all is the runtime
///   `mcp-cimd` module, on by default and only in effect while the `mcp` module is on. kawa fetches a document
///   without following redirects and only from a public address ([UrlPolicy]), and takes the client's redirect URIs
///   from it. Such clients get `mcp` audience tokens only. Switching the module off, or removing a URL from the
///   restriction, revokes the client's tokens; the setting itself is read at startup.
///
/// The UI is always the client `kawa-studio`.
///
/// Besides kawa studio, the authorization server issues tokens to the OAuth clients managed through the admin
/// API ([OAuthClients]). Tokens of a client with audience `mcp` carry `aud = <publicUrl>/mcp` and are accepted only
/// on `POST /mcp`. Disabling or deleting a client, or changing its audience, revokes its tokens.
///
/// Users log in with the authorization code flow with PKCE (no password grant, per OAuth 2.1).
///
/// The issuer is the admin API's public URL: the `iss` claim of every token, the `aud` claim of tokens for the
/// whole admin API, and the authorization server's name in the discovery metadata, which RFC 8414 requires to be
/// the URL that metadata is served under.
///
/// Requires a public URL (`admin.publicUrl`, or the bind address it defaults to) that clients can reach:
/// a wildcard bind address such as `0.0.0.0` is refused, as tokens and metadata would name it. A missing
/// public URL, `loginUrl` or `redirectUris` fails the start with one message naming all of them, as `oauth`
/// is the default `admin.auth.type`: kawa studio usually runs on another origin, so none of them is derived.
///
/// Starts without admin users too. Then nobody can log in until the first one exists: the gateway seeds
/// `admin.auth.bootstrapUser`, or creates the first admin user through its setup endpoint. The
/// authorization flow works meanwhile, so the login UI gets its `login_challenge` and can offer setup.
public final class OAuthAuthenticationProviderFactory implements AuthenticationProviderFactory {

    static final Duration DEFAULT_TOKEN_TTL = Duration.ofMinutes(15);
    static final Duration DEFAULT_SESSION_TTL = Duration.ofHours(8);
    static final String MCP_PATH = "/mcp";

    private final Clock clock;
    private final UrlPolicy urlPolicy;

    /// The production provider: client ID metadata documents are fetched from public `https` hosts only. Integration
    /// tests that serve a document from a local port set the system property [UrlPolicy#TEST_PROPERTY].
    public OAuthAuthenticationProviderFactory() {
        this(Clock.systemUTC(), Boolean.getBoolean(UrlPolicy.TEST_PROPERTY) ? UrlPolicy.LOOPBACK_FOR_TESTS : UrlPolicy.PUBLIC_HTTPS);
    }

    OAuthAuthenticationProviderFactory(Clock clock) {
        this(clock, UrlPolicy.PUBLIC_HTTPS);
    }

    OAuthAuthenticationProviderFactory(UrlPolicy urlPolicy) {
        this(Clock.systemUTC(), urlPolicy);
    }

    private OAuthAuthenticationProviderFactory(Clock clock, UrlPolicy urlPolicy) {
        this.clock = clock;
        this.urlPolicy = urlPolicy;
    }

    @Override
    public String name() {
        return "oauth";
    }

    @Override
    public AuthenticationProvider create(AuthenticationProviderContext context) {
        requireSettings(context);
        var settings = context.settings();
        String issuer = context.publicUrl();
        Duration tokenTtl = positive("tokenTtl", settings.duration("tokenTtl").orElse(DEFAULT_TOKEN_TTL));
        Duration sessionTtl = positive("sessionTtl", settings.duration("sessionTtl").orElse(DEFAULT_SESSION_TTL));
        String loginUrl = absoluteUri("loginUrl", settings.requireString("loginUrl")).toString();
        Set<URI> redirectUris = settings.strings("redirectUris").stream()
                .filter(uri -> !uri.isBlank())
                .map(uri -> absoluteUri("redirectUris", uri))
                .collect(Collectors.toUnmodifiableSet());
        var operator = OperatorClient.from(settings.string("operatorSecret").orElse(null),
                settings.string("operatorActsAs").orElse(OperatorClient.DEFAULT_ACTS_AS));
        Set<String> metadataDocumentRestriction = settings.strings("clientIdMetadataDocuments").stream()
                .map(this::metadataDocumentUrl)
                .collect(Collectors.toUnmodifiableSet());
        String mcpAudience = context.publicUrl() + MCP_PATH;
        Set<String> audiences = Set.copyOf(List.of(issuer, mcpAudience));
        var clients = new OAuthClients(TokenIssuer.STUDIO_CLIENT_ID, redirectUris, context.oauthClients(), issuer, mcpAudience,
                context::clientIdMetadataDocumentsEnabled, metadataDocumentRestriction, new ClientIdMetadataDocuments(clock, urlPolicy));
        var keys = SigningKeys.from(settings.string("signingKey").orElse(null));
        var tokenIssuer = new TokenIssuer(keys, issuer, tokenTtl, sessionTtl, clock);
        var refreshTokens = new TokenVerifier(keys, issuer, audiences, TokenIssuer.REFRESH_TOKEN_TYPE);
        var flow = new AuthorizationFlow(keys, issuer, tokenIssuer, clock);

        context.publicEndpoint("GET", "/oauth/authorize", new AuthorizeEndpoint(flow, clients, loginUrl));
        var adminAccessTokens = new TokenVerifier(keys, issuer, Set.of(issuer), TokenIssuer.ACCESS_TOKEN_TYPE);
        context.publicEndpoint("POST", "/oauth/login", new LoginEndpoint(flow, context.adminUsers(), adminAccessTokens));
        context.publicEndpoint("POST", "/oauth/token", new TokenEndpoint(context.adminUsers(), tokenIssuer, refreshTokens, flow, clients,
                operator, adminAccessTokens));
        context.publicEndpoint("GET", "/.well-known/jwks.json", new JwksEndpoint(keys));
        context.publicEndpoint("GET", "/.well-known/oauth-authorization-server",
                new AuthorizationServerMetadataEndpoint(issuer, context.publicUrl(), context::clientIdMetadataDocumentsEnabled));
        context.publicEndpoint("GET", "/oauth/login-challenge", new LoginChallengeEndpoint(flow, mcpAudience));
        context.publicEndpoint("GET", ProtectedResourceMetadataEndpoint.PATH, new ProtectedResourceMetadataEndpoint(context.publicUrl(), issuer));
        context.publicEndpoint("GET", ProtectedResourceMetadataEndpoint.PATH + MCP_PATH, new ProtectedResourceMetadataEndpoint(mcpAudience, issuer));
        return new KawaTokenAuthenticationProvider(keys, issuer, audiences, clients, context.adminUsers(), operator, context.publicUrl());
    }

    /// Fails with one message naming every required setting that is missing, so a gateway started with the
    /// default `admin.auth.type` learns all it needs at once.
    private static void requireSettings(AuthenticationProviderContext context) {
        var settings = context.settings();
        var missing = new ArrayList<String>();
        if (isWildcard(context.publicUrl())) {
            missing.add("admin.publicUrl (the URL clients reach the admin API at, e.g. https://kawa.example.com; the bind address "
                    + context.publicUrl() + " is a wildcard)");
        }
        if (settings.string("loginUrl").filter(value -> !value.isBlank()).isEmpty()) {
            missing.add("admin.auth.settings.loginUrl (kawa studio's login page, e.g. https://studio.example.com/admin/login)");
        }
        if (settings.strings("redirectUris").stream().allMatch(String::isBlank)) {
            missing.add("admin.auth.settings.redirectUris (kawa studio's callback, e.g. https://studio.example.com/admin/auth/callback)");
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("admin.auth.type 'oauth' (the default) is missing required settings: "
                    + String.join(", ", missing) + ". Set them, or set admin.auth.type: none to run the admin API without authentication.");
        }
    }

    /// Whether `publicUrl` names a wildcard bind address such as `0.0.0.0`, which clients cannot reach.
    private static boolean isWildcard(String publicUrl) {
        String host = URI.create(publicUrl).getHost();
        if (host != null && host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        return host != null && isWildcardAddress(host);
    }

    private static boolean isWildcardAddress(String host) {
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

    private String metadataDocumentUrl(String value) {
        String setting = "clientIdMetadataDocuments";
        URI uri = absoluteUri(setting, value);
        var refusal = urlPolicy.refusal(uri);
        if (refusal.isPresent() || !uri.toString().equals(value)) {
            throw new IllegalArgumentException("admin.auth.settings." + setting + " must be an https URL with a path, without fragment, "
                    + "user info or dot segments: " + value + refusal.map(reason -> " (" + reason + ")").orElse(""));
        }
        return value;
    }

    private static Duration positive(String setting, Duration value) {
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException("admin.auth.settings." + setting + " must be positive");
        }
        return value;
    }
}
