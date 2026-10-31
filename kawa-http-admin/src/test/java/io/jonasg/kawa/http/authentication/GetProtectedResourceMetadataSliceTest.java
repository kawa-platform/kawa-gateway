package io.jonasg.kawa.http.authentication;

import io.jonasg.kawa.http.AdminHttpSliceTestBase;
import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for `GET /.well-known/oauth-protected-resource`.
class GetProtectedResourceMetadataSliceTest extends AdminHttpSliceTestBase {

    @Test
    void describesTheAdminApiAsProtectedResourceWithoutAToken() throws Exception {
        // given
        enableOAuth();
        publicUrl = "https://kawa.example.com";
        startServer();

        // when
        var response = send("GET", "/.well-known/oauth-protected-resource");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body())
                .withFailMessage(() -> "the protected resource metadata does not describe the admin API: " + response.body())
                .isEqualTo("""
                        {
                          "resource": "https://kawa.example.com",
                          "authorization_servers": ["https://kawa.example.com"],
                          "bearer_methods_supported": ["header"]
                        }
                        """);
    }
}
