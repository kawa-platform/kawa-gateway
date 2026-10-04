package io.jonasg.kawa.http;

import com.nimbusds.jose.jwk.JWKSet;
import io.jonasg.kawa.http.auth.oauth.SigningKeys;
import io.jonasg.kawa.test.TestSigningKeys;
import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;

/// Slice tests for `GET /.well-known/jwks.json`.
class GetJwksSliceTest extends AdminHttpSliceTestBase {

    @Test
    void publishesThePublicSigningKey() throws Exception {
        // given
        enableOAuth();
        startServer();

        // when
        var response = send("GET", "/.well-known/jwks.json");

        // then
        assertThat(response).hasStatusCode(200);
        var published = JWKSet.parse(response.body());
        var expected = SigningKeys.from(TestSigningKeys.SEC1_P256).publicJwkSet();
        assertThat(published.toJSONObject(true))
                .withFailMessage(() -> "the JWKS is not the public half of the signing key: " + response.body())
                .isEqualTo(expected.toJSONObject(true));
        assertThat(response.body())
                .withFailMessage(() -> "the JWKS leaks the private key: " + response.body())
                .doesNotContain("\"d\"");
    }
}
