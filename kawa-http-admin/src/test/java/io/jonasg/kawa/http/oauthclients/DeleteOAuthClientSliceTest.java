package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.http.AdminHttpSliceTestBase;
import io.jonasg.kawa.http.FakeGatewayConfigRepository;
import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for [DeleteOAuthClientHandler].
class DeleteOAuthClientSliceTest extends AdminHttpSliceTestBase {

    @Test
    void removesOAuthClientAndPersistsSnapshot() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertOAuthClient(OAuthClients.claude())
                .upsertOAuthClient(OAuthClients.adminTool()));
        startServer();

        // when
        var deleteClientResp = send("DELETE", "/admin/oauth-clients/kawa-mcp-claude?consistency=applied");

        // then
        assertThat(deleteClientResp).hasStatusCode(204);
        assertThat(repository.getActiveConfigOrEmpty().oauthClients())
                .withFailMessage(() -> "OAuth client 'kawa-mcp-claude' was not removed: " + repository.getActiveConfigOrEmpty().oauthClients())
                .containsOnlyKeys("admin-tool");
    }

    @Test
    void missingOAuthClientReturnsNotFound() throws Exception {
        // given
        startServer();

        // when
        var deleteClientResp = send("DELETE", "/admin/oauth-clients/kawa-mcp-claude");

        // then
        assertThat(deleteClientResp).hasStatusCode(404);
        assertThatJson(deleteClientResp.body()).isEqualTo("""
                {"error": "OAuth client 'kawa-mcp-claude' not found"}
                """);
    }

    @Test
    void refusesToDeleteTheReservedKawaStudioClient() throws Exception {
        // given
        startServer();

        // when
        var deleteClientResp = send("DELETE", "/admin/oauth-clients/kawa-studio");

        // then
        assertThat(deleteClientResp).hasStatusCode(409);
        assertThatJson(deleteClientResp.body()).isEqualTo("""
                {"error": "OAuth client id 'kawa-studio' is reserved for kawa studio, which is configured with admin.auth"}
                """);
    }
}
