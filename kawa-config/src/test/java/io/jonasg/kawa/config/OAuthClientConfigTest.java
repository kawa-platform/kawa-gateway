package io.jonasg.kawa.config;

import io.jonasg.kawa.config.OAuthClientConfig.Audience;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OAuthClientConfigTest {

    private static final List<String> REDIRECT_URIS = List.of("http://localhost:*/callback");

    @Test
    void defaultsToAnEnabledClient() {
        // when
        var client = new OAuthClientConfig("kawa-mcp-cursor", null, null, REDIRECT_URIS, Audience.MCP);

        // then
        assertThat(client.enabled())
                .withFailMessage(() -> "an OAuth client without 'enabled' was not enabled")
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"kawa-mcp-claude", "cursor", "0", "ci-bot-2", "a-"})
    void acceptsALowerCaseId(String id) {
        // when
        var client = new OAuthClientConfig(id, null, true, REDIRECT_URIS, Audience.MCP);

        // then
        assertThat(client.id())
                .withFailMessage(() -> "OAuth client id '" + id + "' was not kept")
                .isEqualTo(id);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "-claude", "kawa-mcp-Claude", "kawa_mcp", "kawa mcp", "kawa.mcp"})
    void rejectsAnInvalidId(String id) {
        // when / then
        assertThatThrownBy(() -> new OAuthClientConfig(id, null, true, REDIRECT_URIS, Audience.MCP))
                .withFailMessage(() -> "OAuth client id '" + id + "' was accepted")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("OAuth client id must match");
    }

    @Test
    void acceptsAnIdOf64Characters() {
        // given
        var id = "a".repeat(64);

        // when
        var client = new OAuthClientConfig(id, null, true, REDIRECT_URIS, Audience.MCP);

        // then
        assertThat(client.id())
                .withFailMessage(() -> "a 64-character OAuth client id was not kept")
                .isEqualTo(id);
    }

    @Test
    void rejectsAnIdLongerThan64Characters() {
        // given
        var id = "a".repeat(65);

        // when / then
        assertThatThrownBy(() -> new OAuthClientConfig(id, null, true, REDIRECT_URIS, Audience.MCP))
                .withFailMessage(() -> "a " + id.length() + "-character OAuth client id was accepted")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsAClientWithoutAudience() {
        // when / then
        assertThatThrownBy(() -> new OAuthClientConfig("kawa-mcp-claude", null, true, REDIRECT_URIS, null))
                .withFailMessage(() -> "an OAuth client without audience was accepted")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("OAuth client 'kawa-mcp-claude' needs an audience");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void rejectsAClientWithoutRedirectUris(boolean absent) {
        // given
        List<String> redirectUris = absent ? null : List.of();

        // when / then
        assertThatThrownBy(() -> new OAuthClientConfig("kawa-mcp-claude", null, true, redirectUris, Audience.MCP))
                .withFailMessage(() -> "an OAuth client that cannot redirect anywhere was accepted")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("OAuth client 'kawa-mcp-claude' needs at least one redirect URI");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "https://cursor.example.com/callback",
            "https://cursor.example.com:8443/callback?flow=mcp",
            "http://127.0.0.1:33418/callback",
            "http://[::1]:33418/callback",
            "http://localhost/callback"})
    void acceptsAnHttpsOrLoopbackHttpRedirectUri(String uri) {
        // when
        var client = new OAuthClientConfig("kawa-mcp-claude", null, true, List.of(uri), Audience.MCP);

        // then
        assertThat(client.redirectUris())
                .withFailMessage(() -> "redirect URI '" + uri + "' was not kept")
                .containsExactly(uri);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "http://localhost:*/callback",
            "http://127.0.0.1:*/callback",
            "http://[::1]:*/callback",
            "HTTP://LocalHost:*/callback",
            "http://localhost:*",
            "http://localhost:*/",
            "http://localhost:*/oauth/callback?flow=mcp"})
    void acceptsAPortWildcardOnALoopbackHttpRedirectUriAndKeepsItAsGiven(String uri) {
        // when
        var client = new OAuthClientConfig("kawa-mcp-claude", null, true, List.of(uri), Audience.MCP);

        // then
        assertThat(client.redirectUris())
                .withFailMessage(() -> "port wildcard redirect URI '" + uri + "' was not kept as given: " + client.redirectUris())
                .containsExactly(uri);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "https://localhost:*/callback",
            "http://example.com:*/callback",
            "http://localhost.example.com:*/callback",
            "http://*:8080/callback",
            "http://*/callback",
            "http://localhost:80*/callback",
            "http://localhost:*80/callback",
            "http://localhost:**/callback",
            "http://localhost:*/*",
            "http://localhost:*/call*back",
            "http://localhost:*/callback?flow=*",
            "http://localhost:*@example.com/callback",
            "http://user@localhost:*/callback",
            "http://localhost*/callback",
            "localhost:*/callback"})
    void rejectsAWildcardOutsideTheLoopbackPort(String uri) {
        // when / then
        assertThatThrownBy(() -> new OAuthClientConfig("kawa-mcp-claude", null, true, List.of(uri), Audience.MCP))
                .withFailMessage(() -> "wildcard redirect URI '" + uri + "' was accepted")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("redirect URI '" + uri + "' of OAuth client 'kawa-mcp-claude' may only use * as the entire port "
                        + "of an http URI on 127.0.0.1, [::1] or localhost");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "http://cursor.example.com/callback",
            "https://cursor.example.com/callback#done",
            "http://localhost:*/callback#done",
            "/callback",
            "cursor://callback",
            "https://cursor example.com",
            "http://localhost:*/call back"})
    void rejectsAnInvalidRedirectUri(String uri) {
        // when / then
        assertThatThrownBy(() -> new OAuthClientConfig("kawa-mcp-claude", null, true, List.of(uri), Audience.MCP))
                .withFailMessage(() -> "redirect URI '" + uri + "' was accepted")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("redirect URI '" + uri + "' of OAuth client 'kawa-mcp-claude'");
    }
}
