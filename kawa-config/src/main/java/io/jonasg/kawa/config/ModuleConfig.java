package io.jonasg.kawa.config;

import java.util.Map;

/// Whether a gateway module is switched on, stored in the config topic under the module's id. A
/// module without a stored entry has its default, [ModuleId#enabledByDefault], see [#stateOf].
///
/// @param id      the module
/// @param enabled whether the module is switched on
public record ModuleConfig(ModuleId id, boolean enabled) {

    public ModuleConfig {
        if (id == null) {
            throw new IllegalArgumentException("module id must not be null");
        }
    }

    /// The state of module `id` in a modules section ([DynamicConfig#modules]): its stored entry,
    /// or [ModuleId#enabledByDefault] when it has none. The one place that applies a module's default.
    public static ModuleConfig stateOf(ModuleId id, Map<String, ModuleConfig> modules) {
        return modules.getOrDefault(id.id(), new ModuleConfig(id, id.enabledByDefault()));
    }
}
