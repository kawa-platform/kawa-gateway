package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminUserConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.http.auth.oauth.SigningKeys;
import io.jonasg.kawa.test.TestSigningKeys;
import io.jonasg.kawa.http.auth.oauth.TokenIssuer;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;

/// Slice tests for bearer-token enforcement on the admin API with `admin.auth.type: oauth`.
class AdminAuthenticationSliceTest extends AdminHttpSliceTestBase {

    @Test
    void rejectsRequestWithoutToken() throws Exception {
        // given
        enableOAuth();
        startServer();

        // when
        var response = send("GET", "/topics");

        // then
        assertThat(response).hasStatusCode(401);
        assertThat(response.headers().firstValue("WWW-Authenticate"))
                .withFailMessage(() -> "the 401 carries no bearer challenge: " + response.headers())
                .hasValue("Bearer realm=\"kawa\"");
    }

    @Test
    void admitsRequestWithValidToken() throws Exception {
        // given
        enableOAuth();
        startServer();
        String token = login("admin", "s3cret");

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

        // when
        var response = send("GET", "/topics", null, bearer(foreign.issue("admin").getValue()));

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

        // when
        var response = send("GET", "/topics", null, bearer(otherIssuer.issue("admin").getValue()));

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

        // when
        var response = send("GET", "/topics", null, bearer(expired.issue("admin").getValue()));

        // then
        assertThat(response).hasStatusCode(401);
    }

    @Test
    void rejectsTokenOfDeletedUser() throws Exception {
        // given
        enableOAuth();
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertAdminUser("alice", AdminUserConfig.fromPlaintext("alice-secret")));
        startServer();
        String token = login("alice", "alice-secret");
        send("DELETE", "/admin/users/alice", null, bearer(token));

        // when
        var response = send("GET", "/topics", null, bearer(token));

        // then
        assertThat(response).hasStatusCode(401);
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
}
