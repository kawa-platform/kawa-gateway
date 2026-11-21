package io.jonasg.kawa.mcp;

import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import io.modelcontextprotocol.common.McpTransportContext;

/// Names the caller of an MCP request for the log: the user and the OAuth client the request came through.
final class McpCaller {

    private static final String UNKNOWN = "-";

    private McpCaller() {
    }

    /// The caller in `context` as `<user> via <client>`, with `-` for whichever is not known.
    static String describe(McpTransportContext context) {
        var principal = context.get(McpEndpoint.PRINCIPAL) instanceof KawaPrincipal p ? p : null;
        return (principal == null ? UNKNOWN : principal.name())
                + " via "
                + (principal == null || principal.clientId() == null ? UNKNOWN : principal.clientId());
    }
}
