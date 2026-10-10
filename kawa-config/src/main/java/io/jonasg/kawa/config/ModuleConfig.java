package io.jonasg.kawa.config;

/// Whether a gateway module is switched on, stored in the config topic under the module's id.
///
/// @param id      the module
/// @param enabled whether the module is switched on
public record ModuleConfig(ModuleId id, boolean enabled) {

    public ModuleConfig {
        if (id == null) {
            throw new IllegalArgumentException("module id must not be null");
        }
    }
}
