package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Optional;

/// The gateway's modules: capabilities an operator can switch on and off. The [#id] is the stable
/// identifier used in the config topic and the admin API - never rename it.
public enum ModuleId {
    @JsonProperty("virtual-topics") VIRTUAL_TOPICS("virtual-topics"),
    @JsonProperty("rbac") RBAC("rbac"),
    @JsonProperty("governance") GOVERNANCE("governance");

    private final String id;

    ModuleId(String id) {
        this.id = id;
    }

    /// The stable identifier, e.g. `virtual-topics`.
    public String id() {
        return id;
    }

    /// The module with the given stable identifier, or empty when there is none.
    public static Optional<ModuleId> fromId(String id) {
        return Arrays.stream(values()).filter(module -> module.id.equals(id)).findFirst();
    }
}
