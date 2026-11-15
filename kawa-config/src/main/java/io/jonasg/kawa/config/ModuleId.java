package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Optional;

/// The gateway's modules: capabilities an operator can switch on and off. The [#id] is the stable
/// identifier used in the config topic and the admin API - never rename it. [#enabledByDefault] is
/// the module's state while nothing is stored for it.
public enum ModuleId {
    @JsonProperty("virtual-topics") VIRTUAL_TOPICS("virtual-topics", true),
    @JsonProperty("rbac") RBAC("rbac", true),
    @JsonProperty("governance") GOVERNANCE("governance", true),
    /// The MCP server at `POST /mcp` on the admin port. Off until an operator switches it on.
    @JsonProperty("mcp") MCP("mcp", false),
    /// Lets MCP clients identify themselves with an OAuth Client ID Metadata Document instead of being registered.
    /// Only in effect while [#MCP] is enabled; enabled unless switched off.
    @JsonProperty("mcp-cimd") MCP_CIMD("mcp-cimd", true);

    private final String id;
    private final boolean enabledByDefault;

    ModuleId(String id, boolean enabledByDefault) {
        this.id = id;
        this.enabledByDefault = enabledByDefault;
    }

    /// The stable identifier, e.g. `virtual-topics`.
    public String id() {
        return id;
    }

    /// Whether the module is switched on while nothing is stored for it.
    public boolean enabledByDefault() {
        return enabledByDefault;
    }

    /// The module with the given stable identifier, or empty when there is none.
    public static Optional<ModuleId> fromId(String id) {
        return Arrays.stream(values()).filter(module -> module.id.equals(id)).findFirst();
    }
}
