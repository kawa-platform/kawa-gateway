package io.jonasg.kawa.http.modules;

import io.jonasg.kawa.config.DynamicConfigRepository;
import io.jonasg.kawa.config.ModuleConfig;
import io.jonasg.kawa.config.ModuleId;
import io.jonasg.kawa.http.configchange.ConflictException;
import io.jonasg.kawa.http.configchange.Consistency;
import io.jonasg.kawa.http.configchange.ConsistencyAwareUpdater;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.List;

/// Owns reading and writing the modules section of the gateway config snapshot. Reads report every
/// module's effective state: its stored entry, or its default ([ModuleId#enabledByDefault]) when
/// nothing is stored for it, the same rule the gateway applies ([ModuleConfig#stateOf]).
///
/// The [ModuleId#MCP] module can only be switched on while `admin.publicUrl` is set: it is the URL
/// agents connect to, and without it kawa has none to give them.
public final class ModuleService {

    private final DynamicConfigRepository repository;
    private final ConsistencyAwareUpdater updater;
    private final @Nullable String publicUrl;

    /// @param publicUrl `admin.publicUrl`, or `null` when it is not set: then the mcp module cannot be switched on
    public ModuleService(DynamicConfigRepository repository, @Nullable String publicUrl) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
        this.publicUrl = publicUrl;
    }

    /// Every module the gateway knows, with its effective state.
    List<ModuleConfig> listModules() {
        var modules = repository.getActiveConfigOrEmpty().modules();
        return Arrays.stream(ModuleId.values()).map(id -> ModuleConfig.stateOf(id, modules)).toList();
    }

    /// The effective state of module `id`.
    public ModuleConfig getModule(ModuleId id) {
        return ModuleConfig.stateOf(id, repository.getActiveConfigOrEmpty().modules());
    }

    /// Whether clients may identify themselves with OAuth Client ID Metadata Documents right now: the MCP server
    /// and [ModuleId#MCP_CIMD] are both enabled. Read from the active config on every call, for the `oauth`
    /// provider to ask per request.
    public boolean clientIdMetadataDocumentsEnabled() {
        var modules = repository.getActiveConfigOrEmpty().modules();
        return ModuleConfig.stateOf(ModuleId.MCP, modules).enabled() && ModuleConfig.stateOf(ModuleId.MCP_CIMD, modules).enabled();
    }

    /// Adds or replaces a module. Switching a module off is always allowed.
    ///
    /// @throws ConflictException when switching the mcp module on while `admin.publicUrl` is not set
    ModuleConfig upsertModule(ModuleConfig module, Consistency consistency) {
        if (module.id() == ModuleId.MCP && module.enabled() && publicUrl == null) {
            throw new ConflictException("module 'mcp': set admin.publicUrl, the URL agents connect to, before switching it on");
        }
        updater.update(consistency, config -> config.upsertModule(module));
        return module;
    }
}
