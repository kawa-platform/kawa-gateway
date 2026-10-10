package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.ModuleConfig;
import io.jonasg.kawa.config.ModuleId;

import java.util.Arrays;
import java.util.List;

/// Owns reading and writing the modules section of the gateway config snapshot. Reads report every
/// module's effective state: its stored entry, or enabled when nothing is stored for it, the same
/// rule the gateway applies.
final class ModuleService {

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;

    ModuleService(GatewayConfigRepository repository) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
    }

    /// Every module the gateway knows, with its effective state.
    List<ModuleConfig> listModules() {
        var modules = repository.getActiveConfigOrEmpty().modules();
        return Arrays.stream(ModuleId.values()).map(id -> ModuleConfig.stateOf(id, modules)).toList();
    }

    /// The effective state of module `id`.
    ModuleConfig getModule(ModuleId id) {
        return ModuleConfig.stateOf(id, repository.getActiveConfigOrEmpty().modules());
    }

    /// Adds or replaces a module.
    ModuleConfig upsertModule(ModuleConfig module, Consistency consistency) {
        updater.update(consistency, config -> config.upsertModule(module));
        return module;
    }
}
