package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminUserConfig;
import io.jonasg.kawa.config.GatewayConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for kawa's built-in authorization server over real HTTP: `GET /oauth/authorize`, the
/// login step `POST /oauth/login` for the external login UI, and `POST /oauth/token`.
class PostOAuthTokenSliceTest extends AdminHttpSliceTestBase {

    @Test
    void signsInTheBootstrapUserWithTheAuthorizationCodeFlow() throws Exception {
        // given
        enableOAuth();
        startServer();

        // when
        var response = signIn("admin", "s3cret");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isObject()
                .containsEntry("token_type", "Bearer")
                .containsEntry("expires_in", 900)
                .containsKey("access_token")
                .containsKey("refresh_token");
        assertThat(response.headers().firstValue("Cache-Control"))
                .withFailMessage(() -> "the token response is cacheable: " + response.headers())
                .hasValue("no-store");
    }

    @Test
    void signsInAStoredAdminUser() throws Exception {
        // given
        enableOAuth();
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertAdminUser("alice", AdminUserConfig.fromPlaintext("alice-secret")));
        startServer();

        // when
        var response = signIn("alice", "alice-secret");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isObject().containsKey("access_token");
    }

    @Test
    void redirectsAuthorizeToTheExternalLoginUi() throws Exception {
        // given
        enableOAuth();
        startServer();

        // when
        var response = authorize("some-verifier-that-is-long-enough-for-pkce-0123456789");

        // then
        assertThat(response).hasStatusCode(302);
        assertThat(response.headers().firstValue("Location"))
                .withFailMessage(() -> "authorize did not redirect to the login UI: " + response.headers())
                .hasValueSatisfying(location -> assertThat(location).startsWith(LOGIN_URL + "?login_challenge="));
    }

    @Test
    void rejectsAWrongPasswordAtTheLoginStep() throws Exception {
        // given
        enableOAuth();
        startServer();

        // when
        var response = signIn("admin", "wrong");

        // then
        assertThat(response).hasStatusCode(400);
        assertThatJson(response.body()).isObject().containsEntry("error", "invalid_credentials");
    }

    @Test
    void rejectsAnUnknownUserLikeAWrongPassword() throws Exception {
        // given
        enableOAuth();
        startServer();

        // when
        var response = signIn("mallory", "s3cret");

        // then
        assertThat(response).hasStatusCode(400);
        assertThatJson(response.body()).isEqualTo("""
                {"error":"invalid_credentials","error_description":"invalid username or password"}
                """);
    }

    @Test
    void rejectsThePasswordGrant() throws Exception {
        // given
        enableOAuth();
        startServer();

        // when
        var response = send("POST", "/oauth/token", "grant_type=password&username=admin&password=s3cret",
                Map.of("Content-Type", "application/x-www-form-urlencoded"));

        // then
        assertThat(response).hasStatusCode(400);
        assertThatJson(response.body()).isObject().containsEntry("error", "unsupported_grant_type");
    }

    @Test
    void rejectsRequestWithoutGrantType() throws Exception {
        // given
        enableOAuth();
        startServer();

        // when
        var response = send("POST", "/oauth/token", "code=abc",
                Map.of("Content-Type", "application/x-www-form-urlencoded"));

        // then
        assertThat(response).hasStatusCode(400);
        assertThatJson(response.body()).isObject().containsEntry("error", "invalid_request");
    }

    @Test
    void doesNotExistWithoutOAuth() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/oauth/token", "grant_type=refresh_token&refresh_token=x",
                Map.of("Content-Type", "application/x-www-form-urlencoded"));

        // then
        assertThat(response).hasStatusCode(404);
    }
}
