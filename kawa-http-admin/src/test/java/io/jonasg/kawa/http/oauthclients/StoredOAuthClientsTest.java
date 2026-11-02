package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.OAuthClientConfig;
import io.jonasg.kawa.config.OAuthClientConfig.Audience;
import io.jonasg.kawa.http.FakeGatewayConfigRepository;
import io.jonasg.kawa.plugin.auth.OAuthClient;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;

/// Unit tests for [StoredOAuthClients].
class StoredOAuthClientsTest {

    private final FakeGatewayConfigRepository repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
            .upsertOAuthClient(OAuthClients.claude())
            .upsertOAuthClient(OAuthClients.adminTool()));
    private final StoredOAuthClients directory = new StoredOAuthClients(repository);

    @Test
    void mapsAnMcpClientKeepingThePortWildcard() {
        // when
        var client = directory.find("kawa-mcp-claude");

        // then
        assertThat(client)
                .withFailMessage(() -> "OAuth client 'kawa-mcp-claude' was not mapped as stored: " + client)
                .hasValue(new OAuthClient("kawa-mcp-claude", true, List.of("http://localhost:*/callback"), OAuthClient.Audience.MCP));
    }

    @Test
    void mapsADisabledAdminClient() {
        // when
        var client = directory.find("admin-tool");

        // then
        assertThat(client)
                .withFailMessage(() -> "disabled OAuth client 'admin-tool' was not returned as stored: " + client)
                .hasValue(new OAuthClient("admin-tool", false, List.of("https://tool.example.com/callback"), OAuthClient.Audience.ADMIN));
    }

    @Test
    void findsNothingForAnUnknownId() {
        // when
        var client = directory.find("unknown");

        // then
        assertThat(client)
                .withFailMessage(() -> "an unknown OAuth client was found: " + client)
                .isEmpty();
    }

    @Test
    void seesChangesToTheStoredClientsOnTheNextCall() {
        // given: both clients were looked up before
        directory.find("kawa-mcp-claude");
        directory.find("admin-tool");

        // when
        repository.update(config -> config
                .upsertOAuthClient(new OAuthClientConfig("kawa-mcp-claude", null, false, List.of("http://127.0.0.1:*/cb"), Audience.ADMIN))
                .removeOAuthClient("admin-tool"));

        // then
        assertThat(directory.find("kawa-mcp-claude"))
                .withFailMessage(() -> "the change to OAuth client 'kawa-mcp-claude' was not seen: " + directory.find("kawa-mcp-claude"))
                .hasValue(new OAuthClient("kawa-mcp-claude", false, List.of("http://127.0.0.1:*/cb"), OAuthClient.Audience.ADMIN));
        assertThat(directory.find("admin-tool"))
                .withFailMessage(() -> "the removed OAuth client 'admin-tool' was still found")
                .isEmpty();
    }
}
