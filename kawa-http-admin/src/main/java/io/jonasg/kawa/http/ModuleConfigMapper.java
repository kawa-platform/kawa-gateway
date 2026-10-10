package io.jonasg.kawa.http;

import io.jonasg.kawa.config.ModuleConfig;
import io.jonasg.kawa.config.ModuleId;
import org.jspecify.annotations.Nullable;

/// Converts between the `/modules` transport types and [ModuleConfig].
///
/// A stateless instance created once in [AdminHttpServer] and shared by the module handlers.
final class ModuleConfigMapper {

    /// The module a path addresses.
    ///
    /// @throws NotFoundException when no module has the id
    ModuleId toModuleId(String id) {
        return ModuleId.fromId(id).orElseThrow(() -> new NotFoundException("module '" + id + "' not found"));
    }

    /// A module request as the config record. The module comes from the path.
    ///
    /// @throws IllegalArgumentException when `enabled` is missing
    ModuleConfig toModuleConfig(ModuleId id, @Nullable ModuleRequest req) {
        if (req == null || req.enabled() == null) {
            throw new IllegalArgumentException("module '" + id.id() + "': enabled is required");
        }
        return new ModuleConfig(id, req.enabled());
    }

    /// A single module as the response body.
    ModuleConfigView toModuleConfigView(ModuleConfig config) {
        return new ModuleConfigView(config.id().id(), config.enabled());
    }
}
