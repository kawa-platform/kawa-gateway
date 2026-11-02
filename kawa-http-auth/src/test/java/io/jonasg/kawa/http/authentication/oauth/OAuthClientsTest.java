package io.jonasg.kawa.http.authentication.oauth;

import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationRequest;
import io.jonasg.kawa.plugin.auth.OAuthClient;
import io.jonasg.kawa.plugin.auth.OAuthClient.Audience;
import io.jonasg.kawa.plugin.http.HttpResponse;
import io.jonasg.kawa.test.TestSigningKeys;
import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.jonasg.kawa.http.authentication.oauth.AuthorizationCodeFlow.LOGIN_URL;
import static io.jonasg.kawa.http.authentication.oauth.AuthorizationCodeFlow.REDIRECT_URI;
import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// The authorization server issuing tokens to OAuth clients managed through the admin API, next to kawa studio.
class OAuthClientsTest {

    private static final String PUBLIC_URL = "http://localhost:8080";
    private static final String MCP_AUDIENCE = PUBLIC_URL + "/mcp";
    private static final String MCP_CLIENT = "kawa-mcp-claude";
    private static final String MCP_CALLBACK = "http://localhost:43123/callback";

    private final FakeProviderContext context = new FakeProviderContext(
            new HashMap<>(Map.of("loginUrl", LOGIN_URL, "redirectUris", REDIRECT_URI, "signingKey", TestSigningKeys.SEC1_P256)),
            Map.of("admin", "s3cret"), PUBLIC_URL);
    private final AuthenticationProvider provider;

    OAuthClientsTest() {
        context.oauthClients.put(MCP_CLIENT, new OAuthClient(MCP_CLIENT, true, List.of("http://localhost:*/callback"), Audience.MCP));
        provider = new OAuthAuthenticationProviderFactory().create(context);
    }

    @Test
    void authorizeAcceptsAManagedClientOnAnyLoopbackPort() {
        // given
        var flow = new AuthorizationCodeFlow(context, MCP_CLIENT, MCP_CALLBACK);

        // when
        HttpResponse response = flow.authorize(flow.defaultAuthorization());

        // then
        assertThat(response.status())
                .withFailMessage(() -> "authorize did not accept the managed client: " + body(response))
                .isEqualTo(302);
        assertThat(response.headers().get("Location")).startsWith(LOGIN_URL + "?login_challenge=");
    }

    @Test
    void authorizeRefusesAnUnknownClientWithoutRedirecting() {
        // given
        var flow = new AuthorizationCodeFlow(context, "kawa-mcp-unknown", MCP_CALLBACK);

        // when
        HttpResponse response = flow.authorize(flow.defaultAuthorization());

        // then
        assertRefusedInPlace(response, "an unknown client");
    }

    @Test
    void authorizeRefusesADisabledClientWithoutRedirecting() {
        // given
        context.oauthClients.put(MCP_CLIENT, new OAuthClient(MCP_CLIENT, false, List.of("http://localhost:*/callback"), Audience.MCP));
        var flow = new AuthorizationCodeFlow(context, MCP_CLIENT, MCP_CALLBACK);

        // when
        HttpResponse response = flow.authorize(flow.defaultAuthorization());

        // then
        assertRefusedInPlace(response, "a disabled client");
    }

    @Test
    void authorizeRefusesARedirectUriTheClientDidNotRegister() {
        // given
        var flow = new AuthorizationCodeFlow(context, MCP_CLIENT, "http://localhost:43123/elsewhere");

        // when
        HttpResponse response = flow.authorize(flow.defaultAuthorization());

        // then
        assertRefusedInPlace(response, "an unregistered redirect URI");
    }

    @Test
    void kawaStudioIsResolvedBeforeAManagedClientWithItsId() {
        // given: a managed client claiming kawa studio's id with other redirect URIs and audience
        context.oauthClients.put("kawa-studio", new OAuthClient("kawa-studio", true, List.of("http://localhost:*/callback"), Audience.MCP));
        var impostor = new AuthorizationCodeFlow(context, "kawa-studio", MCP_CALLBACK);
        var studio = new AuthorizationCodeFlow(context);

        // when
        HttpResponse impostorResponse = impostor.authorize(impostor.defaultAuthorization());
        HttpResponse studioTokens = studio.signIn("admin", "s3cret");

        // then
        assertRefusedInPlace(impostorResponse, "the managed client's redirect URI for kawa studio's id");
        String claims = payload(jsonField(studioTokens, "access_token"));
        assertThatJson(claims)
                .withFailMessage(() -> "kawa studio's token did not keep its audience: " + claims)
                .isObject().containsEntry("aud", PUBLIC_URL).containsEntry("client_id", "kawa-studio");
    }

    @Test
    void authorizeReportsAResourceOtherThanTheClientsAudience() {
        // given
        var flow = new AuthorizationCodeFlow(context, MCP_CLIENT, MCP_CALLBACK);
        var params = flow.defaultAuthorization();
        params.put("resource", PUBLIC_URL);

        // when
        HttpResponse response = flow.authorize(params);

        // then
        assertThat(response.status()).isEqualTo(302);
        String location = response.headers().get("Location");
        assertThat(location)
                .withFailMessage(() -> "the resource mismatch was not reported to the client: " + location)
                .startsWith(MCP_CALLBACK + "?");
        assertThat(AuthorizationCodeFlow.queryParam(location, "error")).isEqualTo("invalid_target");
    }

    @Test
    void aManagedMcpClientGetsTokensForTheMcpEndpointThatTheProviderAccepts() {
        // given
        var flow = new AuthorizationCodeFlow(context, MCP_CLIENT, MCP_CALLBACK);
        var params = flow.defaultAuthorization();
        params.put("resource", MCP_AUDIENCE);
        String code = AuthorizationCodeFlow.code(flow.login(AuthorizationCodeFlow.challenge(flow.authorize(params)), "admin", "s3cret"));

        // when
        HttpResponse response = flow.token(exchangeForm(flow, code, MCP_CLIENT, MCP_CALLBACK) + "&resource=" + encode(MCP_AUDIENCE));

        // then
        assertThat(response.status())
                .withFailMessage(() -> "the managed client got no tokens: " + body(response))
                .isEqualTo(200);
        String accessClaims = payload(jsonField(response, "access_token"));
        String refreshClaims = payload(jsonField(response, "refresh_token"));
        assertThatJson(accessClaims)
                .withFailMessage(() -> "the access token is not for the MCP endpoint and client: " + accessClaims)
                .isObject().containsEntry("aud", MCP_AUDIENCE).containsEntry("client_id", MCP_CLIENT).containsEntry("iss", PUBLIC_URL);
        assertThatJson(refreshClaims)
                .withFailMessage(() -> "the refresh token is not for the MCP endpoint and client: " + refreshClaims)
                .isObject().containsEntry("aud", MCP_AUDIENCE).containsEntry("client_id", MCP_CLIENT);
        assertThat(provider.authenticate(bearer(jsonField(response, "access_token"))))
                .withFailMessage(() -> "the provider rejected an MCP access token")
                .isPresent();
    }

    @Test
    void aManagedAdminClientGetsTokensForTheWholeAdminApi() {
        // given
        context.oauthClients.put("kawa-cli", new OAuthClient("kawa-cli", true, List.of("http://127.0.0.1:*/cb"), Audience.ADMIN));

        // when
        HttpResponse response = new AuthorizationCodeFlow(context, "kawa-cli", "http://127.0.0.1:5000/cb").signIn("admin", "s3cret");

        // then
        String claims = payload(jsonField(response, "access_token"));
        assertThatJson(claims)
                .withFailMessage(() -> "the access token is not for the admin API: " + claims)
                .isObject().containsEntry("aud", PUBLIC_URL).containsEntry("client_id", "kawa-cli");
    }

    @Test
    void theTokenEndpointRefusesAResourceOtherThanTheCodesAudience() {
        // given
        var flow = new AuthorizationCodeFlow(context, MCP_CLIENT, MCP_CALLBACK);
        String code = AuthorizationCodeFlow.code(flow.login(
                AuthorizationCodeFlow.challenge(flow.authorize(flow.defaultAuthorization())), "admin", "s3cret"));

        // when
        HttpResponse response = flow.token(exchangeForm(flow, code, MCP_CLIENT, MCP_CALLBACK) + "&resource=" + encode(PUBLIC_URL));

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThatJson(body(response))
                .withFailMessage(() -> "a resource mismatch was not refused as invalid_target: " + body(response))
                .isObject().containsEntry("error", "invalid_target");
    }

    @Test
    void aCodeOfAClientDisabledSinceAuthorizingIsNotRedeemed() {
        // given
        var flow = new AuthorizationCodeFlow(context, MCP_CLIENT, MCP_CALLBACK);
        String code = AuthorizationCodeFlow.code(flow.login(
                AuthorizationCodeFlow.challenge(flow.authorize(flow.defaultAuthorization())), "admin", "s3cret"));
        context.oauthClients.put(MCP_CLIENT, new OAuthClient(MCP_CLIENT, false, List.of("http://localhost:*/callback"), Audience.MCP));

        // when
        HttpResponse response = flow.exchange(code, MCP_CALLBACK);

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThatJson(body(response))
                .withFailMessage(() -> "the code of a disabled client was redeemed: " + body(response))
                .isObject().containsEntry("error", "invalid_grant");
    }

    @Test
    void refreshKeepsTheClientAndAudience() {
        // given
        var flow = new AuthorizationCodeFlow(context, MCP_CLIENT, MCP_CALLBACK);
        String refreshToken = jsonField(flow.signIn("admin", "s3cret"), "refresh_token");

        // when
        HttpResponse response = flow.token("grant_type=refresh_token&refresh_token=" + refreshToken);

        // then
        assertThat(response.status())
                .withFailMessage(() -> "the MCP refresh token was refused: " + body(response))
                .isEqualTo(200);
        String accessClaims = payload(jsonField(response, "access_token"));
        String refreshClaims = payload(jsonField(response, "refresh_token"));
        assertThatJson(accessClaims)
                .withFailMessage(() -> "the refreshed access token lost its audience or client: " + accessClaims)
                .isObject().containsEntry("aud", MCP_AUDIENCE).containsEntry("client_id", MCP_CLIENT);
        assertThatJson(refreshClaims)
                .withFailMessage(() -> "the rotated refresh token lost its audience or client: " + refreshClaims)
                .isObject().containsEntry("aud", MCP_AUDIENCE).containsEntry("client_id", MCP_CLIENT);
    }

    @Test
    void refreshRefusesAResourceOtherThanTheSessionsAudience() {
        // given
        var flow = new AuthorizationCodeFlow(context, MCP_CLIENT, MCP_CALLBACK);
        String refreshToken = jsonField(flow.signIn("admin", "s3cret"), "refresh_token");

        // when
        HttpResponse response = flow.token("grant_type=refresh_token&refresh_token=" + refreshToken + "&resource=" + encode(PUBLIC_URL));

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThatJson(body(response))
                .withFailMessage(() -> "a refresh for another resource was not refused as invalid_target: " + body(response))
                .isObject().containsEntry("error", "invalid_target");
    }

    @Test
    void theProviderRejectsAnAccessTokenForAnotherAudience() {
        // given: a token signed with kawa's key and issuer, but for another resource
        var issuer = new TokenIssuer(SigningKeys.from(TestSigningKeys.SEC1_P256), PUBLIC_URL,
                Duration.ofMinutes(15), Duration.ofHours(8), Clock.systemUTC());
        String foreign = issuer.newSession("admin", "admin", MCP_CLIENT, "https://other.example.com").getAccessToken().getValue();

        // when / then
        assertThat(provider.authenticate(bearer(foreign)))
                .withFailMessage(() -> "the provider accepted a token for a foreign audience")
                .isEmpty();
    }

    private static void assertRefusedInPlace(HttpResponse response, String what) {
        assertThat(response.status())
                .withFailMessage(() -> what + " was not refused with a 400: " + response.status() + " " + response.headers())
                .isEqualTo(400);
        assertThat(response.headers())
                .withFailMessage(() -> what + " was redirected: " + response.headers())
                .doesNotContainKey("Location");
    }

    private static String exchangeForm(AuthorizationCodeFlow flow, String code, String clientId, String redirectUri) {
        return "grant_type=authorization_code&client_id=" + clientId + "&code=" + encode(code)
                + "&redirect_uri=" + encode(redirectUri) + "&code_verifier=" + flow.verifier.getValue();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static AuthenticationRequest bearer(String token) {
        return new AuthenticationRequest("POST", "/mcp", Map.of("Authorization", List.of("Bearer " + token)));
    }

    /// The claims of a JWT, read without verification.
    private static String payload(String jwt) {
        return new String(Base64.getUrlDecoder().decode(jwt.split("\\.")[1]), StandardCharsets.UTF_8);
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
