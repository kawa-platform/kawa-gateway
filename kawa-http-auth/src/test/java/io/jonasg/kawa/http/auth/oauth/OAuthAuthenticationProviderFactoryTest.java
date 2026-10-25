package io.jonasg.kawa.http.auth.oauth;

import io.jonasg.kawa.plugin.auth.AuthenticationRequest;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import io.jonasg.kawa.plugin.http.HttpRequest;
import io.jonasg.kawa.plugin.http.HttpResponse;
import io.jonasg.kawa.test.TestSigningKeys;
import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.jonasg.kawa.http.auth.oauth.AuthorizationCodeFlow.LOGIN_URL;
import static io.jonasg.kawa.http.auth.oauth.AuthorizationCodeFlow.REDIRECT_URI;
import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static io.jonasg.kawa.test.KawaAssertions.assertThatThrownBy;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

class OAuthAuthenticationProviderFactoryTest {

    private final OAuthAuthenticationProviderFactory factory = new OAuthAuthenticationProviderFactory();

    @Test
    void registersTheAuthorizationServerEndpoints() {
        // given
        var context = context(Map.of());

        // when
        factory.create(context);

        // then
        assertThat(context.endpoints.keySet())
                .withFailMessage(() -> "the oauth endpoints were not registered: " + context.endpoints.keySet())
                .containsExactlyInAnyOrder(
                        "GET /oauth/authorize",
                        "POST /oauth/login",
                        "POST /oauth/token",
                        "GET /.well-known/jwks.json",
                        "GET /.well-known/oauth-authorization-server");
    }

    @Test
    void signsInWithTheAuthorizationCodeFlowAndIssuesTokensItsProviderAccepts() {
        // given
        var context = context(Map.of("tokenTtl", "PT5M"));
        var provider = factory.create(context);

        // when
        HttpResponse response = new AuthorizationCodeFlow(context).signIn("admin", "s3cret");

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThatJson(body(response)).isObject().containsEntry("expires_in", 300).containsKey("refresh_token");
        assertThat(provider.authenticate(bearer(jsonField(response, "access_token"))))
                .withFailMessage(() -> "the provider rejected a token issued by its own token endpoint")
                .hasValueSatisfying(principal -> assertThat(principal.name()).isEqualTo("admin"));
    }

    @Test
    void authorizeRedirectsToTheLoginUiWithAChallenge() {
        // given
        var context = context(Map.of());
        factory.create(context);
        var flow = new AuthorizationCodeFlow(context);

        // when
        HttpResponse response = flow.authorize(flow.defaultAuthorization());

        // then
        assertThat(response.status()).isEqualTo(302);
        assertThat(response.headers().get("Location"))
                .withFailMessage(() -> "authorize did not redirect to the login UI: " + response.headers())
                .startsWith(LOGIN_URL + "?login_challenge=");
    }

    @Test
    void authorizeRejectsAnUnregisteredRedirectUriWithoutRedirecting() {
        // given
        var context = context(Map.of());
        factory.create(context);
        var flow = new AuthorizationCodeFlow(context);
        var params = flow.defaultAuthorization();
        params.put("redirect_uri", "https://evil.example/callback");

        // when
        HttpResponse response = flow.authorize(params);

        // then
        assertThat(response.status())
                .withFailMessage(() -> "an unregistered redirect_uri was not refused in place: " + response.headers())
                .isEqualTo(400);
        assertThat(response.headers()).doesNotContainKey("Location");
    }

    @Test
    void authorizeRequiresPkceAndReportsItToTheClient() {
        // given
        var context = context(Map.of());
        factory.create(context);
        var flow = new AuthorizationCodeFlow(context);
        var params = flow.defaultAuthorization();
        params.remove("code_challenge");
        params.remove("code_challenge_method");

        // when
        HttpResponse response = flow.authorize(params);

        // then
        assertThat(response.status()).isEqualTo(302);
        String location = response.headers().get("Location");
        assertThat(location).startsWith(REDIRECT_URI + "?");
        assertThat(AuthorizationCodeFlow.queryParam(location, "error")).isEqualTo("invalid_request");
        assertThat(AuthorizationCodeFlow.queryParam(location, "state")).isEqualTo("xyz");
    }

    @Test
    void loginRejectsBadCredentials() {
        // given
        var context = context(Map.of());
        factory.create(context);
        var flow = new AuthorizationCodeFlow(context);
        String challenge = AuthorizationCodeFlow.challenge(flow.authorize(flow.defaultAuthorization()));

        // when
        HttpResponse response = flow.login(challenge, "admin", "wrong");

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThatJson(body(response)).isObject().containsEntry("error", "invalid_credentials");
    }

    @Test
    void loginRejectsAnExpiredChallenge() {
        // given: a challenge issued 10 minutes ago, with the same signing key
        var settings = Map.of("signingKey", TestSigningKeys.SEC1_P256);
        var tenMinutesAgo = Clock.fixed(Instant.now().minus(Duration.ofMinutes(10)), ZoneOffset.UTC);
        var past = context(settings);
        new OAuthAuthenticationProviderFactory(tenMinutesAgo).create(past);
        var pastFlow = new AuthorizationCodeFlow(past);
        String expired = AuthorizationCodeFlow.challenge(pastFlow.authorize(pastFlow.defaultAuthorization()));
        var context = context(settings);
        factory.create(context);

        // when
        HttpResponse response = new AuthorizationCodeFlow(context).login(expired, "admin", "s3cret");

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThatJson(body(response)).isObject().containsEntry("error", "invalid_challenge");
    }

    @Test
    void anAuthorizationCodeCanBeRedeemedOnlyOnce() {
        // given
        var context = context(Map.of());
        factory.create(context);
        var flow = new AuthorizationCodeFlow(context);
        String code = AuthorizationCodeFlow.code(flow.login(
                AuthorizationCodeFlow.challenge(flow.authorize(flow.defaultAuthorization())), "admin", "s3cret"));
        assertThat(flow.exchange(code, REDIRECT_URI).status()).isEqualTo(200);

        // when
        HttpResponse replay = flow.exchange(code, REDIRECT_URI);

        // then
        assertThat(replay.status()).isEqualTo(400);
        assertThatJson(body(replay)).isObject().containsEntry("error", "invalid_grant");
    }

    @Test
    void anAuthorizationCodeRequiresTheMatchingPkceVerifier() {
        // given: the code was requested with another flow's challenge
        var context = context(Map.of());
        factory.create(context);
        var requester = new AuthorizationCodeFlow(context);
        String code = AuthorizationCodeFlow.code(requester.login(
                AuthorizationCodeFlow.challenge(requester.authorize(requester.defaultAuthorization())), "admin", "s3cret"));

        // when
        HttpResponse response = new AuthorizationCodeFlow(context).exchange(code, REDIRECT_URI);

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThatJson(body(response)).isObject().containsEntry("error", "invalid_grant");
    }

    @Test
    void anAuthorizationCodeRequiresTheSameRedirectUri() {
        // given
        var context = context(Map.of("redirectUris", REDIRECT_URI + ",http://localhost:4173/admin/auth/callback"));
        factory.create(context);
        var flow = new AuthorizationCodeFlow(context);
        String code = AuthorizationCodeFlow.code(flow.login(
                AuthorizationCodeFlow.challenge(flow.authorize(flow.defaultAuthorization())), "admin", "s3cret"));

        // when
        HttpResponse response = flow.exchange(code, "http://localhost:4173/admin/auth/callback");

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThatJson(body(response)).isObject().containsEntry("error", "invalid_grant");
    }

    @Test
    void doesNotSupportThePasswordGrant() {
        // given
        var context = context(Map.of());
        factory.create(context);

        // when
        HttpResponse response = new AuthorizationCodeFlow(context).token("grant_type=password&username=admin&password=s3cret");

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThatJson(body(response)).isObject().containsEntry("error", "unsupported_grant_type");
    }

    @Test
    void theRefreshTokenIsNoBearerToken() {
        // given
        var context = context(Map.of());
        var provider = factory.create(context);

        // when
        String refreshToken = jsonField(new AuthorizationCodeFlow(context).signIn("admin", "s3cret"), "refresh_token");

        // then
        assertThat(provider.authenticate(bearer(refreshToken)))
                .withFailMessage(() -> "a refresh token was accepted as a bearer token")
                .isEmpty();
    }

    @Test
    void refreshIssuesAWorkingAccessTokenAndRotatesTheRefreshTokenWithinTheSession() {
        // given
        var context = context(Map.of("sessionTtl", "PT2H"));
        var provider = factory.create(context);
        var flow = new AuthorizationCodeFlow(context);
        String refreshToken = jsonField(flow.signIn("admin", "s3cret"), "refresh_token");

        // when
        HttpResponse response = flow.token("grant_type=refresh_token&refresh_token=" + refreshToken);

        // then
        assertThat(response.status()).isEqualTo(200);
        String rotated = jsonField(response, "refresh_token");
        assertThat(rotated)
                .withFailMessage(() -> "the refresh token was not rotated")
                .isNotEqualTo(refreshToken);
        assertThat(expiry(rotated))
                .withFailMessage(() -> "refreshing changed the session end")
                .isEqualTo(expiry(refreshToken));
        assertThat(provider.authenticate(bearer(jsonField(response, "access_token"))))
                .withFailMessage(() -> "the refreshed access token was rejected")
                .isPresent();
    }

    @Test
    void rejectsAnExpiredRefreshToken() {
        // given: a session that started 3 hours ago with a 2 hour lifetime
        var settings = Map.of("signingKey", TestSigningKeys.SEC1_P256, "sessionTtl", "PT2H");
        var threeHoursAgo = Clock.fixed(Instant.now().minus(Duration.ofHours(3)), ZoneOffset.UTC);
        String expired = new TokenIssuer(SigningKeys.from(TestSigningKeys.SEC1_P256), "kawa",
                Duration.ofMinutes(15), Duration.ofHours(2), threeHoursAgo)
                .newSession("admin", "admin").getRefreshToken().getValue();
        var context = context(settings);
        factory.create(context);

        // when
        HttpResponse response = new AuthorizationCodeFlow(context).token("grant_type=refresh_token&refresh_token=" + expired);

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThatJson(body(response)).isObject().containsEntry("error", "invalid_grant");
    }

    @Test
    void rejectsARefreshTokenSignedWithAnotherKey() {
        // given
        var other = context(Map.of());
        factory.create(other);
        String foreign = jsonField(new AuthorizationCodeFlow(other).signIn("admin", "s3cret"), "refresh_token");
        var context = context(Map.of("signingKey", TestSigningKeys.SEC1_P256));
        factory.create(context);

        // when
        HttpResponse response = new AuthorizationCodeFlow(context).token("grant_type=refresh_token&refresh_token=" + foreign);

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThatJson(body(response)).isObject().containsEntry("error", "invalid_grant");
    }

    @Test
    void rejectsTheRefreshTokenOfADeletedUser() {
        // given
        var users = new HashMap<>(Map.of("admin", "s3cret", "alice", "alice-secret"));
        var context = new FakeProviderContext(defaults(Map.of()), users);
        factory.create(context);
        var flow = new AuthorizationCodeFlow(context);
        String refreshToken = jsonField(flow.signIn("alice", "alice-secret"), "refresh_token");
        users.remove("alice");

        // when
        HttpResponse response = flow.token("grant_type=refresh_token&refresh_token=" + refreshToken);

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThatJson(body(response)).isObject().containsEntry("error", "invalid_grant");
    }

    @Test
    void rejectsAGatewayWithoutAdminUsers() {
        // given
        var context = new FakeProviderContext(defaults(Map.of()), Map.of());

        // when / then
        assertThatThrownBy(() -> factory.create(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("admin.auth.type 'oauth' requires an admin user: set admin.auth.bootstrapUser to create the first one");
    }

    @Test
    void issuesAnAccessTokenToTheOperatorWithClientSecretBasic() {
        // given
        var context = context(Map.of("operatorSecret", "operator-secret"));
        var provider = factory.create(context);

        // when
        HttpResponse response = tokenWithBasicAuth(context, "kawa-operator", "operator-secret");

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThatJson(body(response)).isObject()
                .containsEntry("token_type", "Bearer")
                .doesNotContainKey("refresh_token");
        assertThat(provider.authenticate(bearer(jsonField(response, "access_token"))))
                .withFailMessage(() -> "the provider rejected the operator's token")
                .hasValue(new KawaPrincipal("kawa-operator", "kawa-operator"));
    }

    @Test
    void issuesAnAccessTokenToTheOperatorWithClientSecretPost() {
        // given
        var context = context(Map.of("operatorSecret", "operator-secret"));
        factory.create(context);

        // when
        HttpResponse response = new AuthorizationCodeFlow(context)
                .token("grant_type=client_credentials&client_id=kawa-operator&client_secret=operator-secret");

        // then
        assertThat(response.status())
                .withFailMessage(() -> "client_secret_post was not accepted: " + body(response))
                .isEqualTo(200);
    }

    @Test
    void rejectsTheOperatorWithTheWrongSecret() {
        // given
        var context = context(Map.of("operatorSecret", "operator-secret"));
        factory.create(context);

        // when
        HttpResponse response = tokenWithBasicAuth(context, "kawa-operator", "wrong");

        // then
        assertThat(response.status()).isEqualTo(401);
        assertThatJson(body(response)).isObject().containsEntry("error", "invalid_client");
    }

    @Test
    void rejectsAnyOtherClientIdEvenWithTheOperatorSecret() {
        // given
        var context = context(Map.of("operatorSecret", "operator-secret"));
        factory.create(context);

        // when
        HttpResponse response = tokenWithBasicAuth(context, "kawa-studio", "operator-secret");

        // then
        assertThat(response.status()).isEqualTo(401);
        assertThatJson(body(response)).isObject().containsEntry("error", "invalid_client");
    }

    @Test
    void issuesNoOperatorTokensWithoutAnOperatorSecret() {
        // given
        var context = context(Map.of());
        factory.create(context);

        // when
        HttpResponse response = tokenWithBasicAuth(context, "kawa-operator", "");

        // then
        assertThat(response.status())
                .withFailMessage(() -> "a token was issued to the operator although it is not configured: " + body(response))
                .isIn(400, 401);
    }

    @Test
    void rejectsTheClientCredentialsGrantWithoutClientAuthentication() {
        // given
        var context = context(Map.of("operatorSecret", "operator-secret"));
        factory.create(context);

        // when
        HttpResponse response = new AuthorizationCodeFlow(context).token("grant_type=client_credentials");

        // then
        assertThat(response.status())
                .withFailMessage(() -> "an unauthenticated client_credentials request was not refused: " + body(response))
                .isIn(400, 401);
        assertThatJson(body(response)).isObject().containsKey("error");
    }

    @Test
    void stopsAcceptingTheOperatorTokenWhenTheOperatorIsRemoved() {
        // given: a token issued while the operator was configured, then a restart without it
        var before = context(Map.of("signingKey", TestSigningKeys.SEC1_P256, "operatorSecret", "operator-secret"));
        factory.create(before);
        String token = jsonField(tokenWithBasicAuth(before, "kawa-operator", "operator-secret"), "access_token");
        var provider = factory.create(context(Map.of("signingKey", TestSigningKeys.SEC1_P256)));

        // when / then
        assertThat(provider.authenticate(bearer(token)))
                .withFailMessage(() -> "the token of a removed operator was still accepted")
                .isEmpty();
    }

    @Test
    void rejectsATokenWithoutAClientId() {
        // given: a validly signed access token for the admin user that names no client
        var context = context(Map.of("signingKey", TestSigningKeys.SEC1_P256));
        var provider = factory.create(context);
        var issuer = new TokenIssuer(SigningKeys.from(TestSigningKeys.SEC1_P256), "kawa", Duration.ofMinutes(15), Duration.ofHours(8),
                Clock.systemUTC());
        Instant now = Instant.now();
        String token = issuer.sign(TokenIssuer.ACCESS_TOKEN_TYPE, issuer.claims(now, now.plus(Duration.ofMinutes(5))).subject("admin").build());

        // when / then
        assertThat(provider.authenticate(bearer(token)))
                .withFailMessage(() -> "a token without a client_id was accepted")
                .isEmpty();
    }

    @Test
    void theOperatorExchangesItsTokenForOneActingAsAnAllowedPrincipal() {
        // given
        var context = context(operatorSettings(Map.of()));
        var provider = factory.create(context);
        String operatorToken = jsonField(tokenWithBasicAuth(context, "kawa-operator", "operator-secret"), "access_token");

        // when
        HttpResponse response = exchange(context, "kawa-operator", "operator-secret", operatorToken, "k8s:team-a");

        // then
        assertThat(response.status())
                .withFailMessage(() -> "the token exchange was refused: " + body(response))
                .isEqualTo(200);
        assertThatJson(body(response)).isObject()
                .containsEntry("issued_token_type", "urn:ietf:params:oauth:token-type:access_token")
                .doesNotContainKey("refresh_token");
        assertThat(provider.authenticate(bearer(jsonField(response, "access_token"))))
                .withFailMessage(() -> "the delegated token was not accepted as k8s:team-a acted by kawa-operator")
                .hasValue(new KawaPrincipal("k8s:team-a", "kawa-operator"));
    }

    @Test
    void refusesAnExchangeForAPrincipalOutsideTheActsAsPattern() {
        // given
        var context = context(operatorSettings(Map.of()));
        factory.create(context);
        String operatorToken = jsonField(tokenWithBasicAuth(context, "kawa-operator", "operator-secret"), "access_token");

        // when
        HttpResponse response = exchange(context, "kawa-operator", "operator-secret", operatorToken, "admin");

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThatJson(body(response)).isObject().containsEntry("error", "unauthorized_client");
    }

    @Test
    void refusesAnExchangeForAnEmailShapedPrincipal() {
        // given: an admin user whose email starts with the pattern's prefix
        var context = context(operatorSettings(Map.of()));
        factory.create(context);
        String operatorToken = jsonField(tokenWithBasicAuth(context, "kawa-operator", "operator-secret"), "access_token");

        // when
        HttpResponse response = exchange(context, "kawa-operator", "operator-secret", operatorToken, "k8s:alice@acme.io");

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThatJson(body(response)).isObject().containsEntry("error", "unauthorized_client");
    }

    @Test
    void refusesToExchangeTheTokenOfAnAdminUser() {
        // given
        var context = context(operatorSettings(Map.of()));
        factory.create(context);
        String userToken = jsonField(new AuthorizationCodeFlow(context).signIn("admin", "s3cret"), "access_token");

        // when
        HttpResponse response = exchange(context, "kawa-operator", "operator-secret", userToken, "k8s:team-a");

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThatJson(body(response)).isObject().containsEntry("error", "invalid_grant");
    }

    @Test
    void refusesToExchangeADelegatedToken() {
        // given: a token already acting as k8s:team-a must not be exchanged again
        var context = context(operatorSettings(Map.of()));
        factory.create(context);
        String operatorToken = jsonField(tokenWithBasicAuth(context, "kawa-operator", "operator-secret"), "access_token");
        String delegated = jsonField(exchange(context, "kawa-operator", "operator-secret", operatorToken, "k8s:team-a"), "access_token");

        // when
        HttpResponse response = exchange(context, "kawa-operator", "operator-secret", delegated, "k8s:team-b");

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThatJson(body(response)).isObject().containsEntry("error", "invalid_grant");
    }

    @Test
    void stopsAcceptingDelegatedTokensWhenTheActsAsPatternNoLongerAllowsThem() {
        // given
        var before = context(operatorSettings(Map.of("signingKey", TestSigningKeys.SEC1_P256)));
        factory.create(before);
        String operatorToken = jsonField(tokenWithBasicAuth(before, "kawa-operator", "operator-secret"), "access_token");
        String delegated = jsonField(exchange(before, "kawa-operator", "operator-secret", operatorToken, "k8s:team-a"), "access_token");
        var provider = factory.create(context(operatorSettings(Map.of("signingKey", TestSigningKeys.SEC1_P256, "operatorActsAs", "k8s:team-b"))));

        // when / then
        assertThat(provider.authenticate(bearer(delegated)))
                .withFailMessage(() -> "a delegated token was accepted after the operator could no longer act as its subject")
                .isEmpty();
    }

    @Test
    void rejectsAnActsAsPatternThatCouldMatchAdminUsers() {
        assertThatThrownBy(() -> factory.create(context(Map.of("operatorSecret", "operator-secret", "operatorActsAs", "*"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("admin.auth.settings.operatorActsAs '*' must be a principal or a prefix ending in '*' with a ':' in it, e.g. 'k8s:*'");
    }

    @Test
    void rejectsABlankOperatorSecret() {
        assertThatThrownBy(() -> factory.create(context(Map.of("operatorSecret", " "))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("admin.auth.settings.operatorSecret must not be blank");
    }

    @Test
    void rejectsNonPositiveTokenTtl() {
        assertThatThrownBy(() -> factory.create(context(Map.of("tokenTtl", "PT0S"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("admin.auth.settings.tokenTtl must be positive");
    }

    @Test
    void requiresTheLoginUiAndRedirectUris() {
        assertThatThrownBy(() -> factory.create(new FakeProviderContext(
                Map.of("redirectUris", REDIRECT_URI), Map.of("admin", "s3cret"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("admin.auth.settings.loginUrl is required");
        assertThatThrownBy(() -> factory.create(new FakeProviderContext(
                Map.of("loginUrl", LOGIN_URL), Map.of("admin", "s3cret"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("admin.auth.settings.redirectUris is required");
    }

    private static FakeProviderContext context(Map<String, String> settings) {
        return new FakeProviderContext(defaults(settings), Map.of("admin", "s3cret"));
    }

    /// `settings` on top of a valid login UI and redirect URI.
    private static Map<String, String> defaults(Map<String, String> settings) {
        var all = new HashMap<>(Map.of("loginUrl", LOGIN_URL, "redirectUris", REDIRECT_URI));
        all.putAll(settings);
        return all;
    }

    /// The `exp` claim of a JWT, read without verification.
    private static long expiry(String jwt) {
        String payload = new String(Base64.getUrlDecoder().decode(jwt.split("\\.")[1]), StandardCharsets.UTF_8);
        int start = payload.indexOf("\"exp\":") + 6;
        int end = start;
        while (end < payload.length() && Character.isDigit(payload.charAt(end))) {
            end++;
        }
        return Long.parseLong(payload.substring(start, end));
    }

    private static HttpResponse tokenWithBasicAuth(FakeProviderContext context, String clientId, String secret) {
        String credentials = Base64.getEncoder().encodeToString((clientId + ":" + secret).getBytes(StandardCharsets.UTF_8));
        return context.endpoints.get("POST /oauth/token").handle(new HttpRequest("POST", "/oauth/token", Map.of(),
                Map.of("content-type", List.of("application/x-www-form-urlencoded"), "authorization", List.of("Basic " + credentials)),
                "grant_type=client_credentials".getBytes(StandardCharsets.UTF_8)));
    }

    /// Settings with the operator enabled (it may act as `k8s:*` by default), on top of `settings`.
    private static Map<String, String> operatorSettings(Map<String, String> settings) {
        var all = new HashMap<>(Map.of("operatorSecret", "operator-secret"));
        all.putAll(settings);
        return all;
    }

    private static HttpResponse exchange(FakeProviderContext context, String clientId, String secret, String subjectToken, String principal) {
        String credentials = Base64.getEncoder().encodeToString((clientId + ":" + secret).getBytes(StandardCharsets.UTF_8));
        String form = "grant_type=" + URLEncoder.encode("urn:ietf:params:oauth:grant-type:token-exchange", StandardCharsets.UTF_8)
                + "&subject_token=" + subjectToken
                + "&subject_token_type=" + URLEncoder.encode("urn:ietf:params:oauth:token-type:access_token", StandardCharsets.UTF_8)
                + "&requested_subject=" + URLEncoder.encode(principal, StandardCharsets.UTF_8);
        return context.endpoints.get("POST /oauth/token").handle(new HttpRequest("POST", "/oauth/token", Map.of(),
                Map.of("content-type", List.of("application/x-www-form-urlencoded"), "authorization", List.of("Basic " + credentials)),
                form.getBytes(StandardCharsets.UTF_8)));
    }

    private static AuthenticationRequest bearer(String token) {
        return new AuthenticationRequest("GET", "/topics", Map.of("Authorization", List.of("Bearer " + token)));
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
