package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.http.AdminHttpSliceTestBase;
import io.jonasg.kawa.http.FakeGatewayConfigRepository;
import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for [GetOAuthClientsHandler].
class GetOAuthClientsSliceTest extends AdminHttpSliceTestBase {

    @Test
    void listsOAuthClientsEmptyWhenNoSnapshotApplied() throws Exception {
        // given
        startServer();

        // when
        var clientsResp = send("GET", "/admin/oauth-clients");

        // then
        assertThat(clientsResp).hasStatusCode(200);
        assertThatJson(clientsResp.body()).isEqualTo("[]");
    }

    @Test
    void listsConfiguredOAuthClientsSortedById() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertOAuthClient(OAuthClients.claude())
                .upsertOAuthClient(OAuthClients.adminTool()));
        startServer();

        // when
        var clientsResp = send("GET", "/admin/oauth-clients");

        // then
        assertThat(clientsResp).hasStatusCode(200);
        assertThatJson(clientsResp.body()).isEqualTo("""
                [
                  {
                    "id": "admin-tool",
                    "name": null,
                    "enabled": false,
                    "redirectUris": ["https://tool.example.com/callback"],
                    "audience": "admin"
                  },
                  {
                    "id": "kawa-mcp-claude",
                    "name": "Claude Code",
                    "enabled": true,
                    "redirectUris": ["http://localhost:*/callback"],
                    "audience": "mcp"
                  }
                ]
                """);
    }
}
