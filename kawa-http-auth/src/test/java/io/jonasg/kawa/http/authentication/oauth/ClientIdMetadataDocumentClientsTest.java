package io.jonasg.kawa.http.authentication.oauth;

import com.sun.net.httpserver.HttpServer;
import io.jonasg.kawa.plugin.auth.AuthenticationRequest;
import io.jonasg.kawa.plugin.http.HttpRequest;
import io.jonasg.kawa.plugin.http.HttpResponse;
import io.jonasg.kawa.test.TestSigningKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.jonasg.kawa.http.authentication.oauth.AuthorizationCodeFlow.LOGIN_URL;
import static io.jonasg.kawa.http.authentication.oauth.AuthorizationCodeFlow.REDIRECT_URI;
import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Clients identified by an OAuth Client ID Metadata Document URL: on while the `enabled` switch is on, for any URL
/// or only those of the optional restriction.
class ClientIdMetadataDocumentClientsTest {

    private static final String PUBLIC_URL = "http://localhost:8080";
    private static final String MCP_AUDIENCE = PUBLIC_URL + "/mcp";
    private static final String CALLBACK = "http://localhost:43123/callback";

    private final AtomicInteger requests = new AtomicInteger();
    private HttpServer server;
    private String clientId;
    private String redirectUris = "http://localhost:*/callback";
    private String clientName = "";

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        clientId = "http://127.0.0.1:" + server.getAddress().getPort() + "/client.json";
        server.createContext("/client.json", exchange -> {
            requests.incrementAndGet();
            var bytes = ("{\"client_id\": \"" + clientId + "\", " + clientName + "\"redirect_uris\": [\"" + redirectUris + "\"]}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void anAllowlistedUrlIsAClientWithTheRedirectUrisOfItsDocument() {
        // given
        var context = context(clientId);
        new OAuthAuthenticationProviderFactory(UrlPolicy.LOOPBACK_FOR_TESTS).create(context);
        var flow = new AuthorizationCodeFlow(context, clientId, CALLBACK);

        // when
        HttpResponse response = flow.authorize(flow.defaultAuthorization());

        // then
        assertThat(response.status())
                .withFailMessage(() -> "authorize did not accept the allowlisted client: " + body(response))
                .isEqualTo(302);
        assertThat(response.headers().get("Location")).startsWith(LOGIN_URL + "?login_challenge=");
    }

    @Test
    void aLoopbackRedirectUriWithoutPortInTheDocumentAllowsAnyPort() {
        // given: a document as Claude Code publishes it
        redirectUris = "http://localhost/callback\", \"http://127.0.0.1/callback";
        var context = context(clientId);
        new OAuthAuthenticationProviderFactory(UrlPolicy.LOOPBACK_FOR_TESTS).create(context);
        var localhost = new AuthorizationCodeFlow(context, clientId, "http://localhost:54321/callback");
        var ip = new AuthorizationCodeFlow(context, clientId, "http://127.0.0.1:54321/callback");
        var otherPath = new AuthorizationCodeFlow(context, clientId, "http://localhost:54321/elsewhere");

        // when
        HttpResponse onLocalhost = localhost.authorize(localhost.defaultAuthorization());
        HttpResponse onIp = ip.authorize(ip.defaultAuthorization());
        HttpResponse elsewhere = otherPath.authorize(otherPath.defaultAuthorization());

        // then
        assertThat(onLocalhost.status()).withFailMessage(() -> "localhost with a port was refused: " + body(onLocalhost)).isEqualTo(302);
        assertThat(onIp.status()).withFailMessage(() -> "127.0.0.1 with a port was refused: " + body(onIp)).isEqualTo(302);
        assertThat(elsewhere.status()).withFailMessage(() -> "another path was accepted").isEqualTo(400);
    }

    @Test
    void aRedirectUriOutsideTheDocumentIsRefused() {
        // given
        var context = context(clientId);
        new OAuthAuthenticationProviderFactory(UrlPolicy.LOOPBACK_FOR_TESTS).create(context);
        var flow = new AuthorizationCodeFlow(context, clientId, "http://localhost:43123/elsewhere");

        // when
        HttpResponse response = flow.authorize(flow.defaultAuthorization());

        // then
        assertThat(response.status())
                .withFailMessage(() -> "a redirect URI the document does not list was not refused: " + response.status())
                .isEqualTo(400);
    }

    @Test
    void aUrlOutsideTheAllowlistIsUnknownAndNeverFetched() {
        // given: another URL is allowlisted, the one asked for is not
        var context = context("https://other.example.com/client.json");
        new OAuthAuthenticationProviderFactory(UrlPolicy.LOOPBACK_FOR_TESTS).create(context);
        var flow = new AuthorizationCodeFlow(context, clientId, CALLBACK);

        // when
        HttpResponse response = flow.authorize(flow.defaultAuthorization());

        // then
        assertThat(response.status()).withFailMessage(() -> "an unlisted client_id was not refused").isEqualTo(400);
        assertThat(requests.get()).withFailMessage(() -> "an unlisted URL was fetched").isZero();
    }

    @Test
    void anAllowlistedClientGetsMcpTokensAndRequestsNeverFetchItsDocument() {
        // given
        var context = context(clientId);
        var provider = new OAuthAuthenticationProviderFactory(UrlPolicy.LOOPBACK_FOR_TESTS).create(context);
        var flow = new AuthorizationCodeFlow(context, clientId, CALLBACK);
        var params = flow.defaultAuthorization();
        params.put("resource", MCP_AUDIENCE);
        String code = AuthorizationCodeFlow.code(flow.login(AuthorizationCodeFlow.challenge(flow.authorize(params)), "admin", "s3cret"));
        HttpResponse tokens = flow.token("grant_type=authorization_code&client_id=" + encode(clientId) + "&code=" + encode(code)
                + "&redirect_uri=" + encode(CALLBACK) + "&code_verifier=" + flow.verifier.getValue() + "&resource=" + encode(MCP_AUDIENCE));
        int fetchesToSignIn = requests.get();

        // when
        var accepted = provider.authenticate(bearer(jsonField(tokens, "access_token")));
        var refreshed = flow.token("grant_type=refresh_token&refresh_token=" + jsonField(tokens, "refresh_token"));

        // then
        String claims = payload(jsonField(tokens, "access_token"));
        assertThatJson(claims)
                .withFailMessage(() -> "the access token is not an MCP token of the client: " + claims)
                .isObject().containsEntry("aud", MCP_AUDIENCE).containsEntry("client_id", clientId);
        assertThat(accepted).withFailMessage(() -> "the provider rejected the CIMD client's token").isPresent();
        assertThat(refreshed.status()).withFailMessage(() -> "refresh failed: " + body(refreshed)).isEqualTo(200);
        assertThat(requests.get())
                .withFailMessage(() -> "authenticating or refreshing fetched the document")
                .isEqualTo(fetchesToSignIn);
    }

    @Test
    void anyUrlIsAClientWithoutARestriction() {
        // given: no restriction
        var context = context(null);
        new OAuthAuthenticationProviderFactory(UrlPolicy.LOOPBACK_FOR_TESTS).create(context);
        var flow = new AuthorizationCodeFlow(context, clientId, CALLBACK);

        // when
        HttpResponse response = flow.authorize(flow.defaultAuthorization());

        // then
        assertThat(response.status()).withFailMessage(() -> "a URL was refused although nothing restricts them: " + body(response)).isEqualTo(302);
    }

    @Test
    void removingTheUrlFromTheRestrictionRevokesTheClientsTokens() {
        // given
        var context = context(clientId);
        var provider = new OAuthAuthenticationProviderFactory(UrlPolicy.LOOPBACK_FOR_TESTS).create(context);
        String accessToken = signInAndGetAccessToken(context);
        var restrictedElsewhere = new OAuthAuthenticationProviderFactory(UrlPolicy.LOOPBACK_FOR_TESTS)
                .create(context("http://127.0.0.1:1/other.json"));

        // when / then
        assertThat(provider.authenticate(bearer(accessToken))).isPresent();
        assertThat(restrictedElsewhere.authenticate(bearer(accessToken)))
                .withFailMessage(() -> "a token of a client outside the restriction was still accepted")
                .isEmpty();
    }

    @Test
    void switchingTheFeatureOffRevokesTokensAndStopsSignInsOnTheNextRequest() {
        // given: a client that signed in while the feature was on
        var context = context(null);
        var provider = new OAuthAuthenticationProviderFactory(UrlPolicy.LOOPBACK_FOR_TESTS).create(context);
        var flow = new AuthorizationCodeFlow(context, clientId, CALLBACK);
        var params = flow.defaultAuthorization();
        params.put("resource", MCP_AUDIENCE);
        String code = AuthorizationCodeFlow.code(flow.login(AuthorizationCodeFlow.challenge(flow.authorize(params)), "admin", "s3cret"));
        HttpResponse tokens = flow.token("grant_type=authorization_code&client_id=" + encode(clientId) + "&code=" + encode(code)
                + "&redirect_uri=" + encode(CALLBACK) + "&code_verifier=" + flow.verifier.getValue());
        String accessToken = jsonField(tokens, "access_token");
        assertThat(provider.authenticate(bearer(accessToken))).isPresent();

        // when: the admin switches it off
        context.clientIdMetadataDocuments = false;

        // then: the token, a refresh, a new authorization and the metadata all follow at once
        assertThat(provider.authenticate(bearer(accessToken)))
                .withFailMessage(() -> "a CIMD token was still accepted after the feature was switched off")
                .isEmpty();
        HttpResponse refreshed = flow.token("grant_type=refresh_token&refresh_token=" + jsonField(tokens, "refresh_token"));
        assertThat(refreshed.status()).withFailMessage(() -> "a CIMD refresh token worked after the feature was switched off").isEqualTo(400);
        var again = new AuthorizationCodeFlow(context, clientId, CALLBACK);
        assertThat(again.authorize(again.defaultAuthorization()).status())
                .withFailMessage(() -> "a new CIMD sign-in started after the feature was switched off")
                .isEqualTo(400);
        assertThatJson(body(metadata(context))).isObject().doesNotContainKey("client_id_metadata_document_supported");

        // and: switching it on again restores them
        context.clientIdMetadataDocuments = true;
        assertThat(provider.authenticate(bearer(accessToken))).withFailMessage(() -> "tokens did not come back").isPresent();
        assertThatJson(body(metadata(context))).isObject().containsEntry("client_id_metadata_document_supported", true);
    }

    @Test
    void theLoginChallengeSaysWhoIsAskingAndIsVerified() {
        // given: an unregistered client with a name, and a registered client
        clientName = "\"client_name\": \"Acme Agent\", ";
        var context = context(null);
        new OAuthAuthenticationProviderFactory(UrlPolicy.LOOPBACK_FOR_TESTS).create(context);
        var cimd = new AuthorizationCodeFlow(context, clientId, CALLBACK);
        var params = cimd.defaultAuthorization();
        params.put("resource", MCP_AUDIENCE);
        String cimdChallenge = AuthorizationCodeFlow.challenge(cimd.authorize(params));
        var studio = new AuthorizationCodeFlow(context);
        String studioChallenge = AuthorizationCodeFlow.challenge(studio.authorize(studio.defaultAuthorization()));

        // when
        HttpResponse unregistered = loginChallenge(context, cimdChallenge);
        HttpResponse registered = loginChallenge(context, studioChallenge);
        HttpResponse tampered = loginChallenge(context, cimdChallenge.substring(0, cimdChallenge.length() - 3) + "abc");
        HttpResponse missing = loginChallenge(context, null);

        // then
        assertThat(unregistered.status()).withFailMessage(() -> "login-challenge failed: " + body(unregistered)).isEqualTo(200);
        assertThatJson(body(unregistered)).isObject()
                .containsEntry("clientId", clientId).containsEntry("clientName", "Acme Agent").containsEntry("redirectHost", "localhost")
                .containsEntry("audience", "mcp").containsEntry("registered", false);
        assertThatJson(body(registered)).isObject()
                .containsEntry("clientId", "kawa-studio").containsEntry("clientName", "kawa-studio")
                .containsEntry("audience", "admin").containsEntry("registered", true);
        assertThat(tampered.status()).withFailMessage(() -> "a tampered challenge was described").isEqualTo(400);
        assertThat(missing.status()).withFailMessage(() -> "a missing challenge was described").isEqualTo(400);
    }

    @Test
    void theNameFallsBackToTheClientIdWhenTheDocumentHasNone() {
        // given
        var context = context(null);
        new OAuthAuthenticationProviderFactory(UrlPolicy.LOOPBACK_FOR_TESTS).create(context);
        var flow = new AuthorizationCodeFlow(context, clientId, CALLBACK);
        String challenge = AuthorizationCodeFlow.challenge(flow.authorize(flow.defaultAuthorization()));

        // then
        assertThatJson(body(loginChallenge(context, challenge))).isObject().containsEntry("clientName", clientId);
    }

    private String signInAndGetAccessToken(FakeProviderContext context) {
        var flow = new AuthorizationCodeFlow(context, clientId, CALLBACK);
        var params = flow.defaultAuthorization();
        params.put("resource", MCP_AUDIENCE);
        String code = AuthorizationCodeFlow.code(flow.login(AuthorizationCodeFlow.challenge(flow.authorize(params)), "admin", "s3cret"));
        return jsonField(flow.token("grant_type=authorization_code&client_id=" + encode(clientId) + "&code=" + encode(code)
                + "&redirect_uri=" + encode(CALLBACK) + "&code_verifier=" + flow.verifier.getValue()), "access_token");
    }

    private static HttpResponse loginChallenge(FakeProviderContext context, String challenge) {
        var query = challenge == null ? Map.<String, String>of() : Map.of("login_challenge", encode(challenge));
        return context.endpoints.get("GET /oauth/login-challenge")
                .handle(new HttpRequest("GET", "/oauth/login-challenge", query, Map.of(), new byte[0]));
    }

    private static HttpResponse metadata(FakeProviderContext context) {
        return context.endpoints.get("GET /.well-known/oauth-authorization-server")
                .handle(new HttpRequest("GET", "/.well-known/oauth-authorization-server", Map.of(),
                        Map.of("host", List.of("localhost:8080")), new byte[0]));
    }

    private static FakeProviderContext context(String allowlisted) {
        var settings = new HashMap<>(Map.of("loginUrl", LOGIN_URL, "redirectUris", REDIRECT_URI, "signingKey", TestSigningKeys.SEC1_P256));
        if (allowlisted != null) {
            settings.put("clientIdMetadataDocuments", allowlisted);
        }
        return new FakeProviderContext(settings, Map.of("admin", "s3cret"), PUBLIC_URL);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static AuthenticationRequest bearer(String token) {
        return new AuthenticationRequest("POST", "/mcp", Map.of("Authorization", List.of("Bearer " + token)));
    }

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
