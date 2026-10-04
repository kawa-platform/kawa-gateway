package io.jonasg.kawa.http;

import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for `GET /.well-known/oauth-authorization-server`.
class GetAuthorizationServerMetadataSliceTest extends AdminHttpSliceTestBase {

    @Test
    void describesTheBuiltInAuthorizationServer() throws Exception {
        // given
        enableOAuth();
        startServer();
        String base = "http://127.0.0.1:" + server().boundPort();

        // when
        var response = send("GET", "/.well-known/oauth-authorization-server");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isObject()
                .containsEntry("issuer", "kawa-test")
                .containsEntry("authorization_endpoint", base + "/oauth/authorize")
                .containsEntry("token_endpoint", base + "/oauth/token")
                .containsEntry("jwks_uri", base + "/.well-known/jwks.json");
        assertThatJson(response.body()).node("code_challenge_methods_supported").isArray().containsExactly("S256");
        assertThatJson(response.body()).node("grant_types_supported").isArray().containsExactly("authorization_code", "refresh_token");
    }
}
