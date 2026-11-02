package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.http.AdminHttpSliceTestBase;
import io.jonasg.kawa.http.FakeGatewayConfigRepository;
import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for [GetOAuthClientHandler].
class GetOAuthClientSliceTest extends AdminHttpSliceTestBase {

    @Test
    void getsAStoredOAuthClient() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty().upsertOAuthClient(OAuthClients.claude()));
        startServer();

        // when
        var clientResp = send("GET", "/admin/oauth-clients/kawa-mcp-claude");

        // then
        assertThat(clientResp).hasStatusCode(200);
        assertThatJson(clientResp.body()).isEqualTo("""
                {
                  "id": "kawa-mcp-claude",
                  "name": "Claude Code",
                  "enabled": true,
                  "redirectUris": ["http://localhost:*/callback"],
                  "audience": "mcp"
                }
                """);
    }

    @Test
    void missingOAuthClientReturnsNotFound() throws Exception {
        // given
        startServer();

        // when
        var clientResp = send("GET", "/admin/oauth-clients/kawa-mcp-claude");

        // then
        assertThat(clientResp).hasStatusCode(404);
        assertThatJson(clientResp.body()).isEqualTo("""
                {"error": "OAuth client 'kawa-mcp-claude' not found"}
                """);
    }
}
