package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.ModuleConfig;
import io.jonasg.kawa.config.ModuleId;

import java.util.Collection;

/// Owns reading and writing the modules section of the gateway config snapshot.
final class ModuleService {

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;

    ModuleService(GatewayConfigRepository repository) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
    }

    /// Every stored module.
    Collection<ModuleConfig> listModules() {
        return repository.getActiveConfigOrEmpty().modules().values();
    }

    /// The module stored under `id`.
    ///
    /// @throws NotFoundException if the module is not stored
    ModuleConfig getModule(ModuleId id) {
        var module = repository.getActiveConfigOrEmpty().modules().get(id.id());
        if (module == null) {
            throw new NotFoundException("module '" + id.id() + "' not found");
        }
        return module;
    }

    /// Adds or replaces a module.
    ModuleConfig upsertModule(ModuleConfig module, Consistency consistency) {
        updater.update(consistency, config -> config.upsertModule(module));
        return module;
    }

    /// Removes the module stored under `id`.
    ///
    /// @throws NotFoundException if the module is not stored
    void deleteModule(ModuleId id, Consistency consistency) {
        getModule(id);
        updater.update(consistency, config -> config.removeModule(id));
    }
}
