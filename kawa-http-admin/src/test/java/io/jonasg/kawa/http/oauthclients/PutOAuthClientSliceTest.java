package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.config.DynamicConfig;
import io.jonasg.kawa.config.OAuthClientConfig;
import io.jonasg.kawa.config.OAuthClientConfig.Audience;
import io.jonasg.kawa.http.AdminHttpSliceTestBase;
import io.jonasg.kawa.http.FakeDynamicConfigRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for [PutOAuthClientHandler].
class PutOAuthClientSliceTest extends AdminHttpSliceTestBase {

    @Test
    void addsOAuthClientAndPersistsSnapshot() throws Exception {
        // given
        startServer();

        // when
        var putClientResp = send("PUT", "/admin/oauth-clients/kawa-mcp-claude", """
                {
                  "name": "Claude Code",
                  "redirectUris": ["http://localhost:*/callback"],
                  "audience": "mcp"
                }
                """);

        // then
        assertThat(putClientResp).hasStatusCode(200);
        assertThatJson(putClientResp.body()).isEqualTo("""
                {
                  "id": "kawa-mcp-claude",
                  "name": "Claude Code",
                  "enabled": true,
                  "redirectUris": ["http://localhost:*/callback"],
                  "audience": "mcp"
                }
                """);
        assertThat(repository.getActiveConfigOrEmpty().oauthClients().get("kawa-mcp-claude"))
                .withFailMessage(() -> "OAuth client 'kawa-mcp-claude' was not stored: " + repository.getActiveConfigOrEmpty().oauthClients())
                .isEqualTo(OAuthClients.claude());
    }

    @Test
    void replacesAStoredOAuthClient() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().upsertOAuthClient(OAuthClients.claude()));
        startServer();

        // when
        var putClientResp = send("PUT", "/admin/oauth-clients/kawa-mcp-claude?consistency=applied", """
                {
                  "enabled": false,
                  "redirectUris": ["https://claude.example.com/callback"],
                  "audience": "admin"
                }
                """);

        // then
        assertThat(putClientResp).hasStatusCode(200);
        var expected = new OAuthClientConfig("kawa-mcp-claude", null, false, List.of("https://claude.example.com/callback"), Audience.ADMIN);
        assertThat(repository.getActiveConfigOrEmpty().oauthClients().get("kawa-mcp-claude"))
                .withFailMessage(() -> "OAuth client 'kawa-mcp-claude' was not replaced: " + repository.getActiveConfigOrEmpty().oauthClients())
                .isEqualTo(expected);
    }

    @Test
    void takesTheIdFromThePathNotTheBody() throws Exception {
        // given
        startServer();

        // when
        var putClientResp = send("PUT", "/admin/oauth-clients/kawa-mcp-claude", """
                {
                  "id": "kawa-mcp-other",
                  "redirectUris": ["http://localhost:*/callback"],
                  "audience": "mcp"
                }
                """);

        // then
        assertThat(putClientResp).hasStatusCode(200);
        assertThatJson(putClientResp.body()).inPath("id").isEqualTo("kawa-mcp-claude");
        assertThat(repository.getActiveConfigOrEmpty().oauthClients())
                .withFailMessage(() -> "OAuth client was not stored under the path id: " + repository.getActiveConfigOrEmpty().oauthClients())
                .containsOnlyKeys("kawa-mcp-claude");
    }

    @Test
    void refusesTheReservedKawaStudioClient() throws Exception {
        // given
        startServer();

        // when
        var putClientResp = send("PUT", "/admin/oauth-clients/kawa-studio", """
                {
                  "redirectUris": ["https://studio.example.com/callback"],
                  "audience": "admin"
                }
                """);

        // then
        assertThat(putClientResp).hasStatusCode(409);
        assertThatJson(putClientResp.body()).isEqualTo("""
                {"error": "OAuth client id 'kawa-studio' is reserved for kawa studio, which is configured with admin.auth"}
                """);
        assertThat(repository.getActiveConfigOrEmpty().oauthClients())
                .withFailMessage(() -> "the reserved client was stored: " + repository.getActiveConfigOrEmpty().oauthClients())
                .isEmpty();
    }

    @Test
    void refusesTheReservedKawaOperatorClient() throws Exception {
        // given
        startServer();

        // when
        var putClientResp = send("PUT", "/admin/oauth-clients/kawa-operator", """
                {
                  "redirectUris": ["https://operator.example.com/callback"],
                  "audience": "admin"
                }
                """);

        // then
        assertThat(putClientResp).hasStatusCode(409);
        assertThatJson(putClientResp.body()).isEqualTo("""
                {"error": "OAuth client id 'kawa-operator' is reserved for the kawa operator, which is configured with admin.auth.settings.operatorSecret"}
                """);
        assertThat(repository.getActiveConfigOrEmpty().oauthClients())
                .withFailMessage(() -> "the reserved client was stored: " + repository.getActiveConfigOrEmpty().oauthClients())
                .isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', textBlock = """
            invalid id | Kawa_MCP | {"redirectUris":["http://localhost:*/callback"],"audience":"mcp"} | OAuth client id must match
            missing audience | kawa-mcp-claude | {"redirectUris":["http://localhost:*/callback"]} | OAuth client 'kawa-mcp-claude' needs an audience
            unsupported audience | kawa-mcp-claude | {"redirectUris":["http://localhost:*/callback"],"audience":"MCP"} | unsupported audience 'MCP'
            missing redirect URIs | kawa-mcp-claude | {"audience":"mcp"} | OAuth client 'kawa-mcp-claude' needs at least one redirect URI
            plain http redirect URI | kawa-mcp-claude | {"redirectUris":["http://claude.example.com/cb"],"audience":"mcp"} | must use https
            wildcard port on https | kawa-mcp-claude | {"redirectUris":["https://localhost:*/cb"],"audience":"mcp"} | may only use * as the entire port
            malformed body | kawa-mcp-claude | {"redirectUris": | invalid OAuth client body
            """)
    void rejectsAnInvalidOAuthClient(String description, String id, String body, String message) throws Exception {
        // given
        startServer();

        // when
        var putClientResp = send("PUT", "/admin/oauth-clients/" + id, body);

        // then
        assertThat(putClientResp).hasStatusCode(400);
        assertThat(putClientResp.body())
                .withFailMessage(() -> description + ": expected an error containing \"" + message + "\" but got " + putClientResp.body())
                .contains(message);
        assertThat(repository.getActiveConfigOrEmpty().oauthClients())
                .withFailMessage(() -> description + ": an invalid OAuth client was stored: " + repository.getActiveConfigOrEmpty().oauthClients())
                .isEmpty();
    }
}
