package io.jonasg.kawa.http.authentication;

import io.jonasg.kawa.config.DynamicConfig;
import io.jonasg.kawa.config.OAuthClientConfig;
import io.jonasg.kawa.config.OAuthClientConfig.Audience;
import io.jonasg.kawa.http.AdminHttpSliceTestBase;
import io.jonasg.kawa.http.FakeDynamicConfigRepository;
import io.jonasg.kawa.http.authentication.oauth.SigningKeys;
import io.jonasg.kawa.http.authentication.oauth.TokenIssuer;
import io.jonasg.kawa.http.web.Router;
import io.jonasg.kawa.test.TestSigningKeys;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;

import static io.jonasg.kawa.http.AdminUserConfigMother.ALICE_ID;
import static io.jonasg.kawa.http.AdminUserConfigMother.BOB_ID;
import static io.jonasg.kawa.http.AdminUserConfigMother.adminUser;
import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for bearer-token enforcement on the admin API with `admin.auth.type: oauth`.
class AdminAuthenticationSliceTest extends AdminHttpSliceTestBase {

    @Test
    void rejectsRequestWithoutToken() throws Exception {
        // given
        enableOAuth();
        publicUrl = "https://kawa.example.com";
        startServer();

        // when
        var response = send("GET", "/topics");

        // then
        assertThat(response).hasStatusCode(401);
        assertThat(response.headers().firstValue("WWW-Authenticate"))
                .withFailMessage(() -> "the 401 does not point at the protected resource metadata: " + response.headers())
                .hasValue("Bearer resource_metadata=\"https://kawa.example.com/.well-known/oauth-protected-resource\"");
    }

    @Test
    void admitsRequestWithValidToken() throws Exception {
        // given
        enableOAuth();
        startServer();
        String token = login("admin@kawa", "s3cret");

        // when
        var response = send("GET", "/topics", null, bearer(token));

        // then
        assertThat(response).hasStatusCode(200);
    }

    @Test
    void rejectsMalformedToken() throws Exception {
        // given
        enableOAuth();
        startServer();

        // when
        var response = send("GET", "/topics", null, bearer("not-a-jwt"));

        // then
        assertThat(response).hasStatusCode(401);
    }

    @Test
    void rejectsTokenSignedWithAnotherKey() throws Exception {
        // given
        enableOAuth();
        startServer();
        var foreign = new TokenIssuer(SigningKeys.generate(), "kawa-test", Duration.ofMinutes(15), Duration.ofHours(8), Clock.systemUTC());
        String token = foreign.newSession("admin@kawa", "admin", "kawa-studio", "kawa-test").getAccessToken().getValue();

        // when
        var response = send("GET", "/topics", null, bearer(token));

        // then
        assertThat(response).hasStatusCode(401);
    }

    @Test
    void rejectsTokenFromAnotherIssuer() throws Exception {
        // given
        enableOAuth();
        startServer();
        var otherIssuer = new TokenIssuer(SigningKeys.from(TestSigningKeys.SEC1_P256), "someone-else",
                Duration.ofMinutes(15), Duration.ofHours(8), Clock.systemUTC());
        String token = otherIssuer.newSession("admin@kawa", "admin", "kawa-studio", "kawa-test").getAccessToken().getValue();

        // when
        var response = send("GET", "/topics", null, bearer(token));

        // then
        assertThat(response).hasStatusCode(401);
    }

    @Test
    void rejectsExpiredToken() throws Exception {
        // given
        enableOAuth();
        startServer();
        var anHourAgo = Clock.fixed(Instant.now().minus(Duration.ofHours(1)), ZoneOffset.UTC);
        var expired = new TokenIssuer(SigningKeys.from(TestSigningKeys.SEC1_P256), "kawa-test",
                Duration.ofMinutes(15), Duration.ofHours(8), anHourAgo);
        String token = expired.newSession("admin@kawa", "admin", "kawa-studio", "kawa-test").getAccessToken().getValue();

        // when
        var response = send("GET", "/topics", null, bearer(token));

        // then
        assertThat(response).hasStatusCode(401);
    }

    @Test
    void rejectsTokenOfDeletedUser() throws Exception {
        // given
        enableOAuth();
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withPassword("alice-secret").build())
                .upsertAdminUser(BOB_ID, adminUser().withEmail("bob@mycom.com").withPassword("bob-secret").build()));
        startServer();
        String token = login("alice@mycom.com", "alice-secret");
        var deleted = send("DELETE", "/admin/users/" + ALICE_ID, null, bearer(login("bob@mycom.com", "bob-secret")));
        assertThat(deleted).hasStatusCode(204);

        // when
        var response = send("GET", "/topics", null, bearer(token));

        // then
        assertThat(response).hasStatusCode(401);
    }

    @Test
    void rejectsTokenOfDisabledUser() throws Exception {
        // given
        enableOAuth();
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withPassword("alice-secret").build())
                .upsertAdminUser(BOB_ID, adminUser().withEmail("bob@mycom.com").withPassword("bob-secret").build()));
        startServer();
        String aliceToken = login("alice@mycom.com", "alice-secret");
        String adminToken = login("bob@mycom.com", "bob-secret");
        var disabled = send("PATCH", "/admin/users/" + ALICE_ID, """
                {"enabled":false}
                """, bearer(adminToken));
        assertThat(disabled).hasStatusCode(200);

        // when
        var response = send("GET", "/topics", null, bearer(aliceToken));

        // then
        assertThat(response).hasStatusCode(401);
    }

    @Test
    void acceptsTokenAgainOnceTheUserIsReEnabled() throws Exception {
        // given
        enableOAuth();
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withPassword("alice-secret").build())
                .upsertAdminUser(BOB_ID, adminUser().withEmail("bob@mycom.com").withPassword("bob-secret").build()));
        startServer();
        String aliceToken = login("alice@mycom.com", "alice-secret");
        String adminToken = login("bob@mycom.com", "bob-secret");
        send("PATCH", "/admin/users/" + ALICE_ID, """
                {"enabled":false}
                """, bearer(adminToken));
        send("PATCH", "/admin/users/" + ALICE_ID, """
                {"enabled":true}
                """, bearer(adminToken));

        // when
        var response = send("GET", "/topics", null, bearer(aliceToken));

        // then
        assertThat(response).hasStatusCode(200);
    }

    @Test
    void keepsTokenValidWhenTheEmailChanges() throws Exception {
        // given: tokens identify the admin user by id, not by email
        enableOAuth();
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withPassword("alice-secret").build()));
        startServer();
        String aliceToken = login("alice@mycom.com", "alice-secret");
        var renamed = send("PATCH", "/admin/users/" + ALICE_ID, """
                {"email":"alice.liddell@mycom.com"}
                """, bearer(aliceToken));
        assertThat(renamed).hasStatusCode(200);

        // when
        var response = send("GET", "/topics", null, bearer(aliceToken));

        // then
        assertThat(response).hasStatusCode(200);
    }

    @Test
    void keepsPublicEndpointsReachableWithoutToken() throws Exception {
        // given
        enableOAuth();
        startServer();

        // when
        var docs = send("GET", "/docs");
        var jwks = send("GET", "/.well-known/jwks.json");

        // then
        assertThat(docs).hasStatusCode(200);
        assertThat(jwks).hasStatusCode(200);
    }

    @Test
    void staysOpenWithoutOAuth() throws Exception {
        // given
        startServer();

        // when
        var response = send("GET", "/topics");

        // then
        assertThat(response).hasStatusCode(200);
    }

    @Test
    void acceptsAnMcpClientsTokenOnlyOnPostMcp() throws Exception {
        // given: an MCP client, and a stand-in for kawa-mcp's endpoint at POST /mcp
        enableOAuth();
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().upsertOAuthClient(new OAuthClientConfig(
                "kawa-mcp-claude", "Claude Code", true, List.of("http://localhost:*/callback"), Audience.MCP)));
        startServer();
        server().router().post("/mcp", request -> {
            var caller = new HashMap<String, String>();
            caller.put("name", request.principalName());
            caller.put("clientId", request.principal() == null ? null : request.principal().clientId());
            return Router.Response.ok(caller);
        });
        var tokenResp = signIn("kawa-mcp-claude", "http://localhost:43123/callback", "admin@kawa", "s3cret");
        assertThat(tokenResp).hasStatusCode(200);
        String token = JsonMapper.builder().build().readTree(tokenResp.body()).get("access_token").asString();

        // when
        var mcpResp = send("POST", "/mcp", "{}", bearer(token));
        var topicsResp = send("GET", "/topics", null, bearer(token));

        // then
        assertThat(mcpResp).hasStatusCode(200);
        assertThatJson(mcpResp.body())
                .withFailMessage(() -> "POST /mcp did not see the MCP client's caller: " + mcpResp.body())
                .isEqualTo("""
                        {"name": "admin@kawa", "clientId": "kawa-mcp-claude"}""");
        assertThat(topicsResp).hasStatusCode(401);
        assertThat(topicsResp.headers().firstValue("WWW-Authenticate"))
                .withFailMessage(() -> "the 401 for the MCP token on GET /topics carries no challenge: " + topicsResp.headers())
                .hasValueSatisfying(challenge -> assertThat(challenge).startsWith("Bearer resource_metadata="));
    }
}
