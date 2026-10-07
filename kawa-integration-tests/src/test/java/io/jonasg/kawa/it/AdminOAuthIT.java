package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AdminAuthConfig;
import io.jonasg.kawa.config.AdminConfig;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end OAuth on the admin API of a real gateway: kawa issues tokens for the bootstrap user
/// and for admin users stored in the config topic, and rejects requests without one.
class AdminOAuthIT extends AdminHTTPTestSupport {

    private static final String REDIRECT_URI = "http://localhost:5173/admin/auth/callback";

    @Override
    protected AdminConfig adminConfig() {
        return new AdminConfig(true, "127.0.0.1", 0, null,
                new AdminAuthConfig("oauth", new AdminAuthConfig.BootstrapUser("admin@kawa", "s3cret"), Map.of(
                        "loginUrl", "http://localhost:5173/admin/login",
                        "redirectUris", REDIRECT_URI)));
    }

    @Test
    void rejectsAdminRequestsWithoutToken() throws Exception {
        // when
        HttpResponse<String> response = httpExec(reqBuilder("/topics").GET().build());

        // then
        assertThat(response.statusCode())
                .withFailMessage(() -> "GET /topics without a token was not rejected: " + response.statusCode())
                .isEqualTo(401);
    }

    @Test
    void bootstrapUserCallsTheAdminApiWithAToken() throws Exception {
        // given
        String token = login("admin@kawa", "s3cret");

        // when
        HttpResponse<String> response = httpExec(reqBuilder("/topics").GET()
                .header("Authorization", "Bearer " + token).build());

        // then
        assertThat(response.statusCode())
                .withFailMessage(() -> "GET /topics with a valid token failed: " + response.body())
                .isEqualTo(200);
    }

    @Test
    void adminUserCreatedThroughTheApiCanLogIn() throws Exception {
        // given: the bootstrap admin creates 'alice@mycom.com', persisted in and applied from the config topic
        String adminToken = login("admin@kawa", "s3cret");
        HttpResponse<String> created = httpExec(reqBuilder("/admin/users?consistency=applied")
                .POST(HttpRequest.BodyPublishers.ofString("{\"email\":\"alice@mycom.com\",\"password\":\"alice-secret\"}"))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + adminToken).build());
        assertThat(created.statusCode())
                .withFailMessage(() -> "creating admin user 'alice@mycom.com' failed: " + created.body())
                .isEqualTo(201);

        // when
        String aliceToken = login("alice@mycom.com", "alice-secret");
        HttpResponse<String> response = httpExec(reqBuilder("/admin/users").GET()
                .header("Authorization", "Bearer " + aliceToken).build());

        // then
        assertThat(response.statusCode())
                .withFailMessage(() -> "GET /admin/users as 'alice@mycom.com' failed: " + response.body())
                .isEqualTo(200);
        assertThat(response.body()).contains("\"alice@mycom.com\"");
    }

    @Test
    void refreshTokenObtainsANewAccessToken() throws Exception {
        // given
        String refreshToken = signIn("admin@kawa", "s3cret").get("refresh_token").asString();

        // when
        String accessToken = tokenResponse("grant_type=refresh_token&refresh_token=" + refreshToken)
                .get("access_token").asString();
        HttpResponse<String> response = httpExec(reqBuilder("/topics").GET()
                .header("Authorization", "Bearer " + accessToken).build());

        // then
        assertThat(response.statusCode())
                .withFailMessage(() -> "GET /topics with a refreshed access token failed: " + response.body())
                .isEqualTo(200);
    }

    private String login(String username, String password) throws Exception {
        return signIn(username, password).get("access_token").asString();
    }

    /// The authorization code flow with PKCE, as kawa studio and the browser run it: authorize (redirect
    /// to the login UI with a challenge), log in, then redeem the code.
    private JsonNode signIn(String username, String password) throws Exception {
        String verifier = "verifier-" + UUID.randomUUID() + "-" + UUID.randomUUID();
        String challenge = Base64.getUrlEncoder().withoutPadding().encodeToString(
                MessageDigest.getInstance("SHA-256").digest(verifier.getBytes(StandardCharsets.US_ASCII)));
        HttpResponse<String> authorize = httpExec(reqBuilder("/oauth/authorize?response_type=code&client_id=kawa-studio"
                + "&redirect_uri=" + encode(REDIRECT_URI) + "&code_challenge=" + challenge
                + "&code_challenge_method=S256&state=it").GET().build());
        String loginChallenge = queryParam(authorize.headers().firstValue("Location")
                .orElseThrow(() -> new AssertionError("authorize did not redirect: " + authorize.body())), "login_challenge");
        HttpResponse<String> login = httpExec(reqBuilder("/oauth/login")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JsonMapper.builder().build().writeValueAsString(Map.of(
                        "login_challenge", loginChallenge, "username", username, "password", password))))
                .build());
        assertThat(login.statusCode())
                .withFailMessage(() -> "login as '" + username + "' failed: " + login.body())
                .isEqualTo(200);
        String code = queryParam(JsonMapper.builder().build().readTree(login.body()).get("redirect_to").asString(), "code");
        return tokenResponse("grant_type=authorization_code&client_id=kawa-studio&code=" + encode(code)
                + "&redirect_uri=" + encode(REDIRECT_URI) + "&code_verifier=" + verifier);
    }

    private static String queryParam(String url, String name) {
        return Arrays.stream(URI.create(url).getRawQuery().split("&"))
                .map(pair -> pair.split("=", 2))
                .filter(pair -> pair[0].equals(name))
                .map(pair -> URLDecoder.decode(pair[1], StandardCharsets.UTF_8))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no " + name + " in " + url));
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private JsonNode tokenResponse(String form) throws Exception {
        HttpResponse<String> response = httpExec(reqBuilder("/oauth/token")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form))
                .build());
        assertThat(response.statusCode())
                .withFailMessage(() -> "token request failed: " + response.body())
                .isEqualTo(200);
        return JsonMapper.builder().build().readTree(response.body());
    }
}
