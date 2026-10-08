package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.Base64;
import java.util.Map;

import static io.jonasg.kawa.http.AdminUserConfigMother.ALICE_ID;
import static io.jonasg.kawa.http.AdminUserConfigMother.BOB_ID;
import static io.jonasg.kawa.http.AdminUserConfigMother.adminUser;
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
        var response = signIn("admin@kawa", "s3cret");

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
                .upsertAdminUser(ALICE_ID, adminUser().withPassword("alice-secret").build()));
        startServer();

        // when
        var response = signIn("alice@mycom.com", "alice-secret");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isObject().containsKey("access_token");
    }

    @Test
    void rejectsADisabledAdminUserLikeAWrongPassword() throws Exception {
        // given
        enableOAuth();
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withPassword("alice-secret").disabled().build()));
        startServer();

        // when
        var response = signIn("alice@mycom.com", "alice-secret");

        // then
        assertThat(response).hasStatusCode(400);
        assertThatJson(response.body()).isEqualTo("""
                {"error":"invalid_credentials","error_description":"invalid username or password"}
                """);
    }

    @Test
    void refusesToRefreshTheSessionOfADisabledAdminUser() throws Exception {
        // given
        enableOAuth();
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withPassword("alice-secret").build())
                .upsertAdminUser(BOB_ID, adminUser().withEmail("bob@mycom.com").withPassword("bob-secret").build()));
        startServer();
        String refreshToken = JsonMapper.builder().build()
                .readTree(signIn("alice@mycom.com", "alice-secret").body()).get("refresh_token").asString();
        String adminToken = login("bob@mycom.com", "bob-secret");
        send("PATCH", "/admin/users/" + ALICE_ID, """
                {"enabled":false}
                """, bearer(adminToken));

        // when
        var response = send("POST", "/oauth/token", "grant_type=refresh_token&refresh_token=" + refreshToken,
                Map.of("Content-Type", "application/x-www-form-urlencoded"));

        // then
        assertThat(response).hasStatusCode(400);
        assertThatJson(response.body()).isObject().containsEntry("error", "invalid_grant");
    }

    @Test
    void signsInWithTheEmailInAnyCase() throws Exception {
        // given
        enableOAuth();
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withEmail("alice@mycom.com").withPassword("alice-secret").build()));
        startServer();

        // when
        var response = signIn("Alice@MyCom.com", "alice-secret");

        // then
        assertThat(response).hasStatusCode(200);
    }

    @Test
    void accessTokenIdentifiesTheUserByIdAndNamesThemByEmail() throws Exception {
        // given
        enableOAuth();
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withEmail("alice@mycom.com").withPassword("alice-secret").build()));
        startServer();

        // when
        String accessToken = JsonMapper.builder().build()
                .readTree(signIn("alice@mycom.com", "alice-secret").body()).get("access_token").asString();

        // then
        var claims = JsonMapper.builder().build().readTree(
                Base64.getUrlDecoder().decode(accessToken.split("\\.")[1]));
        assertThat(claims.get("sub").asString())
                .withFailMessage(() -> "the access token does not identify the admin user by id: " + claims)
                .isEqualTo(ALICE_ID);
        assertThat(claims.get("preferred_username").asString())
                .withFailMessage(() -> "the access token does not name the admin user by email: " + claims)
                .isEqualTo("alice@mycom.com");
    }

    @Test
    void seededBootstrapUserIsIdentifiedByTheirIdLikeAnyAdminUser() throws Exception {
        // given
        enableOAuth();
        startServer();

        // when
        String accessToken = JsonMapper.builder().build()
                .readTree(signIn("admin@kawa", "s3cret").body()).get("access_token").asString();

        // then
        var claims = JsonMapper.builder().build().readTree(
                Base64.getUrlDecoder().decode(accessToken.split("\\.")[1]));
        assertThat(claims.get("sub").asString())
                .withFailMessage(() -> "the seeded bootstrap user is not identified by their id: " + claims)
                .isEqualTo(BootstrapUserSeeder.idOf("admin@kawa"));
        assertThat(claims.get("preferred_username").asString()).isEqualTo("admin@kawa");
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
        var response = signIn("admin@kawa", "wrong");

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
        var response = signIn("mallory@mycom.com", "s3cret");

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
        var response = send("POST", "/oauth/token", "grant_type=password&username=admin%40kawa&password=s3cret",
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
