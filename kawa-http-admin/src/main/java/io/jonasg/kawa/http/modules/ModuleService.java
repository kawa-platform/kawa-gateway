package io.jonasg.kawa.http.modules;

import io.jonasg.kawa.config.DynamicConfigRepository;
import io.jonasg.kawa.config.ModuleConfig;
import io.jonasg.kawa.config.ModuleId;
import io.jonasg.kawa.http.configchange.Consistency;
import io.jonasg.kawa.http.configchange.ConsistencyAwareUpdater;

import java.util.Arrays;
import java.util.List;

/// Owns reading and writing the modules section of the gateway config snapshot. Reads report every
/// module's effective state: its stored entry, or enabled when nothing is stored for it, the same
/// rule the gateway applies.
public final class ModuleService {

    private final DynamicConfigRepository repository;
    private final ConsistencyAwareUpdater updater;

    public ModuleService(DynamicConfigRepository repository) {
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

    /// Whether clients may identify themselves with OAuth Client ID Metadata Documents right now: the MCP server
    /// and [ModuleId#MCP_CIMD] are both enabled. Read from the active config on every call, for the `oauth`
    /// provider to ask per request.
    public boolean clientIdMetadataDocumentsEnabled() {
        var modules = repository.getActiveConfigOrEmpty().modules();
        return ModuleConfig.stateOf(ModuleId.MCP, modules).enabled() && ModuleConfig.stateOf(ModuleId.MCP_CIMD, modules).enabled();
    }

    /// Adds or replaces a module.
    ModuleConfig upsertModule(ModuleConfig module, Consistency consistency) {
        updater.update(consistency, config -> config.upsertModule(module));
        return module;
    }
}
