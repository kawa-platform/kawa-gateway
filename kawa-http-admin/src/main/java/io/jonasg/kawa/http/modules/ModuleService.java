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
/// The [ModuleId#MCP] module is only available while `admin.publicUrl` is set: it is the URL agents
/// connect to, and without it kawa has none to give them. An unavailable module cannot be switched
/// on, but one that is on (stored before `admin.publicUrl` was removed) can always be switched off.
public final class ModuleService {

    /// The path the MCP server is served at on the admin port, `KawaMcpServer.PATH` in kawa-mcp.
    private static final String MCP_PATH = "/mcp";

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

    /// Why module `id` cannot run on this gateway, or `null` when it can. Independent of whether it is
    /// switched on.
    @Nullable String unavailableReason(ModuleId id) {
        if (id == ModuleId.MCP && publicUrl == null) {
            return "set admin.publicUrl, the URL agents connect to";
        }
        return null;
    }

    /// The URL agents connect to for `module`, `<admin.publicUrl>/mcp`: only for the mcp module while it
    /// is on and available, otherwise `null`.
    @Nullable String connectUrl(ModuleConfig module) {
        if (module.id() != ModuleId.MCP || !module.enabled() || publicUrl == null) {
            return null;
        }
        return publicUrl + MCP_PATH;
    }

    /// Adds or replaces a module. Switching a module off is always allowed.
    ///
    /// @throws ConflictException when switching on a module that is not available, see [#unavailableReason]
    ModuleConfig upsertModule(ModuleConfig module, Consistency consistency) {
        String unavailableReason = unavailableReason(module.id());
        if (module.enabled() && unavailableReason != null) {
            throw new ConflictException("module '" + module.id().id() + "': " + unavailableReason + ", before switching it on");
        }
        updater.update(consistency, config -> config.upsertModule(module));
        return module;
    }
}
