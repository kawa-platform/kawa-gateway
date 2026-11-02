package io.jonasg.kawa.http.authentication.oauth;

import com.nimbusds.jose.JOSEObjectType;
import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationRequest;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import io.jonasg.kawa.plugin.auth.OAuthClient;
import io.jonasg.kawa.plugin.auth.OAuthClient.Audience;
import io.jonasg.kawa.plugin.http.HttpResponse;
import io.jonasg.kawa.test.TestSigningKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.jonasg.kawa.http.authentication.oauth.AuthorizationCodeFlow.LOGIN_URL;
import static io.jonasg.kawa.http.authentication.oauth.AuthorizationCodeFlow.REDIRECT_URI;
import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Where kawa's access tokens are accepted, and how disabling, deleting or changing their OAuth client revokes them.
class KawaTokenAuthenticationProviderTest {

    private static final String PUBLIC_URL = "http://localhost:8080";
    private static final String MCP_CLIENT = "kawa-mcp-claude";
    private static final String MCP_CALLBACK = "http://localhost:43123/callback";
    private static final List<String> MCP_REDIRECTS = List.of("http://localhost:*/callback");

    private final FakeProviderContext context = new FakeProviderContext(
            new HashMap<>(Map.of("loginUrl", LOGIN_URL, "redirectUris", REDIRECT_URI, "signingKey", TestSigningKeys.SEC1_P256)),
            Map.of("admin", "s3cret"), PUBLIC_URL);
    private final AuthenticationProvider provider;

    KawaTokenAuthenticationProviderTest() {
        context.oauthClients.put(MCP_CLIENT, new OAuthClient(MCP_CLIENT, true, MCP_REDIRECTS, Audience.MCP));
        provider = new OAuthAuthenticationProviderFactory().create(context);
    }

    @Test
    void acceptsAnMcpTokenOnPostMcp() {
        // given
        String token = mcpTokens().accessToken();

        // when
        Optional<KawaPrincipal> principal = provider.authenticate(request("POST", "/mcp", token));

        // then
        assertThat(principal)
                .withFailMessage(() -> "the MCP token was not accepted on POST /mcp")
                .hasValue(new KawaPrincipal("admin", MCP_CLIENT));
    }

    @ParameterizedTest
    @CsvSource({"GET, /mcp", "POST, /mcp/", "GET, /topics", "POST, /admin/oauth-clients", "DELETE, /mcp"})
    void rejectsAnMcpTokenAnywhereButPostMcp(String method, String path) {
        // given
        String token = mcpTokens().accessToken();

        // when
        Optional<KawaPrincipal> principal = provider.authenticate(request(method, path, token));

        // then
        assertThat(principal)
                .withFailMessage(() -> "the MCP token was accepted on " + method + " " + path)
                .isEmpty();
    }

    @ParameterizedTest
    @CsvSource({"GET, /topics", "POST, /mcp", "POST, /admin/oauth-clients"})
    void acceptsAKawaStudioTokenOnTheWholeAdminApi(String method, String path) {
        // given
        String token = accessToken(new AuthorizationCodeFlow(context).signIn("admin", "s3cret"));

        // when
        Optional<KawaPrincipal> principal = provider.authenticate(request(method, path, token));

        // then
        assertThat(principal)
                .withFailMessage(() -> "kawa studio's token was not accepted on " + method + " " + path)
                .hasValue(new KawaPrincipal("admin", "kawa-studio"));
    }

    @Test
    void refreshesARefreshTokenWithoutClientIdAsKawaStudios() {
        // given: a refresh token issued before tokens named their client
        String refreshToken = withoutClientId(TokenIssuer.REFRESH_TOKEN_TYPE);

        // when
        HttpResponse response = new AuthorizationCodeFlow(context).token("grant_type=refresh_token&refresh_token=" + refreshToken);

        // then
        assertThat(response.status())
                .withFailMessage(() -> "a refresh token without client_id was refused: " + body(response))
                .isEqualTo(200);
        assertThat(provider.authenticate(request("GET", "/topics", accessToken(response))))
                .withFailMessage(() -> "the refreshed token was not kawa studio's")
                .hasValue(new KawaPrincipal("admin", "kawa-studio"));
    }

    @Test
    void disablingAClientRevokesItsTokensUntilItIsEnabledAgain() {
        // given
        var tokens = mcpTokens();
        context.oauthClients.put(MCP_CLIENT, new OAuthClient(MCP_CLIENT, false, MCP_REDIRECTS, Audience.MCP));

        // when
        Optional<KawaPrincipal> whileDisabled = provider.authenticate(request("POST", "/mcp", tokens.accessToken()));
        HttpResponse refresh = tokens.refresh();
        context.oauthClients.put(MCP_CLIENT, new OAuthClient(MCP_CLIENT, true, MCP_REDIRECTS, Audience.MCP));
        Optional<KawaPrincipal> reEnabled = provider.authenticate(request("POST", "/mcp", tokens.accessToken()));

        // then
        assertThat(whileDisabled)
                .withFailMessage(() -> "the access token of a disabled client was accepted")
                .isEmpty();
        assertInvalidGrant(refresh, "a disabled client");
        assertThat(reEnabled)
                .withFailMessage(() -> "the access token was not accepted again once its client was re-enabled")
                .isPresent();
    }

    @Test
    void deletingAClientRevokesItsTokens() {
        // given
        var tokens = mcpTokens();
        context.oauthClients.remove(MCP_CLIENT);

        // when
        Optional<KawaPrincipal> principal = provider.authenticate(request("POST", "/mcp", tokens.accessToken()));
        HttpResponse refresh = tokens.refresh();

        // then
        assertThat(principal)
                .withFailMessage(() -> "the access token of a deleted client was accepted")
                .isEmpty();
        assertInvalidGrant(refresh, "a deleted client");
    }

    @Test
    void changingAClientsAudienceRevokesItsTokens() {
        // given
        var tokens = mcpTokens();
        context.oauthClients.put(MCP_CLIENT, new OAuthClient(MCP_CLIENT, true, MCP_REDIRECTS, Audience.ADMIN));

        // when
        Optional<KawaPrincipal> principal = provider.authenticate(request("POST", "/mcp", tokens.accessToken()));
        HttpResponse refresh = tokens.refresh();

        // then
        assertThat(principal)
                .withFailMessage(() -> "an MCP token was accepted after its client moved to the admin audience")
                .isEmpty();
        assertInvalidGrant(refresh, "a client with another audience");
    }

    @Test
    void anAdminClientsTokensStopWorkingOnceItMovesToMcp() {
        // given
        context.oauthClients.put("kawa-cli", new OAuthClient("kawa-cli", true, List.of("http://127.0.0.1:*/cb"), Audience.ADMIN));
        String token = accessToken(new AuthorizationCodeFlow(context, "kawa-cli", "http://127.0.0.1:5000/cb").signIn("admin", "s3cret"));
        context.oauthClients.put("kawa-cli", new OAuthClient("kawa-cli", true, List.of("http://127.0.0.1:*/cb"), Audience.MCP));

        // when
        Optional<KawaPrincipal> principal = provider.authenticate(request("POST", "/mcp", token));

        // then
        assertThat(principal)
                .withFailMessage(() -> "an admin token was accepted after its client moved to the MCP audience")
                .isEmpty();
    }

    @Test
    void kawaStudioIsUnaffectedByAManagedClientWithItsId() {
        // given
        String token = accessToken(new AuthorizationCodeFlow(context).signIn("admin", "s3cret"));
        context.oauthClients.put("kawa-studio", new OAuthClient("kawa-studio", false, MCP_REDIRECTS, Audience.MCP));

        // when
        Optional<KawaPrincipal> principal = provider.authenticate(request("GET", "/topics", token));

        // then
        assertThat(principal)
                .withFailMessage(() -> "kawa studio's token was revoked by a disabled managed client with its id")
                .isPresent();
    }

    @Test
    void theOneArgumentPrincipalHasNoClient() {
        // when
        var principal = new KawaPrincipal("api-key-client");

        // then
        assertThat(principal.clientId())
                .withFailMessage(() -> "a principal without a client named one: " + principal)
                .isNull();
    }

    /// A kawa session token of `type` for the admin API that, like those issued before tokens named their client,
    /// has no `client_id`.
    private static String withoutClientId(JOSEObjectType type) {
        var issuer = new TokenIssuer(SigningKeys.from(TestSigningKeys.SEC1_P256), PUBLIC_URL,
                Duration.ofMinutes(15), Duration.ofHours(8), Clock.systemUTC());
        Instant now = Instant.now();
        return issuer.sign(type, issuer.claims(now, now.plus(Duration.ofMinutes(5))).subject("admin").build());
    }

    private Tokens mcpTokens() {
        var flow = new AuthorizationCodeFlow(context, MCP_CLIENT, MCP_CALLBACK);
        HttpResponse response = flow.signIn("admin", "s3cret");
        assertThat(response.status())
                .withFailMessage(() -> "the MCP client got no tokens: " + body(response))
                .isEqualTo(200);
        return new Tokens(flow, jsonField(response, "access_token"), jsonField(response, "refresh_token"));
    }

    private record Tokens(AuthorizationCodeFlow flow, String accessToken, String refreshToken) {

        HttpResponse refresh() {
            return flow.token("grant_type=refresh_token&refresh_token=" + refreshToken);
        }
    }

    private static void assertInvalidGrant(HttpResponse response, String what) {
        assertThat(response.status())
                .withFailMessage(() -> "the refresh token of " + what + " was not refused: " + body(response))
                .isEqualTo(400);
        assertThatJson(body(response)).isObject().containsEntry("error", "invalid_grant");
    }

    private static AuthenticationRequest request(String method, String path, String token) {
        return new AuthenticationRequest(method, path, Map.of("Authorization", List.of("Bearer " + token)));
    }

    private static String accessToken(HttpResponse response) {
        return jsonField(response, "access_token");
    }

    private static String body(HttpResponse response) {
        return new String(response.body(), StandardCharsets.UTF_8);
    }

    private static String jsonField(HttpResponse response, String field) {
        String json = body(response);
        int start = json.indexOf("\"" + field + "\":\"") + field.length() + 4;
        return json.substring(start, json.indexOf('"', start));
    }
}
