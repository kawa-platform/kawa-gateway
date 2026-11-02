package io.jonasg.kawa.http.oauthclients;

import io.jonasg.kawa.config.OAuthClientConfig;
import io.jonasg.kawa.config.OAuthClientConfig.Audience;

import java.util.List;

/// OAuth clients for the `/admin/oauth-clients` slice tests.
final class OAuthClients {

    private OAuthClients() {
    }

    /// Claude Code: any loopback port, tokens only for `/mcp`.
    static OAuthClientConfig claude() {
        return new OAuthClientConfig("kawa-mcp-claude", "Claude Code", true, List.of("http://localhost:*/callback"), Audience.MCP);
    }

    /// An admin tool on a fixed https callback, switched off.
    static OAuthClientConfig adminTool() {
        return new OAuthClientConfig("admin-tool", null, false, List.of("https://tool.example.com/callback"), Audience.ADMIN);
    }
}
