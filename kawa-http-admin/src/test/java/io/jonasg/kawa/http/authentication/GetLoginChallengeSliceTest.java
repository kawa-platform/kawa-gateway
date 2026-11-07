package io.jonasg.kawa.http.authentication;

import io.jonasg.kawa.http.AdminHttpSliceTestBase;
import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// `GET /oauth/login-challenge`: who is asking, as the login UI shows it before the credentials, read from the
/// verified login challenge.
class GetLoginChallengeSliceTest extends AdminHttpSliceTestBase {

    private static final String MCP_REDIRECT = "http://localhost:43123/callback";

    @Test
    void describesKawaStudioAsARegisteredAdminClient() throws Exception {
        // given
        enableOAuth();
        startServer();
        String challenge = challengeFor("kawa-studio", REDIRECT_URI);

        // when
        var response = send("GET", "/oauth/login-challenge?login_challenge=" + encode(challenge));

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isObject()
                .containsEntry("clientId", "kawa-studio").containsEntry("clientName", "kawa-studio")
                .containsEntry("redirectHost", "localhost").containsEntry("audience", "admin").containsEntry("registered", true);
        assertThat(response.headers().firstValue("Cache-Control"))
                .withFailMessage(() -> "the answer is cacheable: " + response.headers())
                .contains("no-store");
    }

    @Test
    void describesAStoredMcpClientByItsIdAndAudience() throws Exception {
        // given
        enableOAuth();
        startServer();
        var admin = bearer(login("admin@kawa", "s3cret"));
        var stored = send("PUT", "/admin/oauth-clients/claude-code", """
                {"redirectUris":["http://localhost:*/callback"],"audience":"mcp"}
                """, admin);
        assertThat(stored).hasStatusCode(200);
        String challenge = challengeFor("claude-code", MCP_REDIRECT);

        // when
        var response = send("GET", "/oauth/login-challenge?login_challenge=" + encode(challenge));

        // then
        assertThatJson(response.body()).isObject()
                .containsEntry("clientId", "claude-code").containsEntry("clientName", "claude-code")
                .containsEntry("redirectHost", "localhost").containsEntry("audience", "mcp").containsEntry("registered", true);
    }

    @Test
    void isPublicButRefusesAChallengeThatIsMissingOrTamperedWith() throws Exception {
        // given
        enableOAuth();
        startServer();
        String challenge = challengeFor("kawa-studio", REDIRECT_URI);

        // when
        var missing = send("GET", "/oauth/login-challenge");
        var tampered = send("GET", "/oauth/login-challenge?login_challenge=" + encode(challenge.substring(0, challenge.length() - 3) + "abc"));
        var garbage = send("GET", "/oauth/login-challenge?login_challenge=not-a-jwt");

        // then
        for (var response : java.util.List.of(missing, tampered, garbage)) {
            assertThat(response).hasStatusCode(400);
            assertThatJson(response.body()).isObject().containsEntry("error", "invalid_challenge");
        }
    }

    private String challengeFor(String clientId, String redirectUri) throws Exception {
        return queryParam(authorize(clientId, redirectUri, "verifier-" + java.util.UUID.randomUUID() + "-xxxxxxxxxxxxxxxxxxxx")
                .headers().firstValue("Location").orElseThrow(), "login_challenge");
    }
}
