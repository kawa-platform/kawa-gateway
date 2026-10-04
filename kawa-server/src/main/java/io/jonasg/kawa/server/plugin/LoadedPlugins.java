package io.jonasg.kawa.server.plugin;

import io.jonasg.kawa.http.auth.AuthenticationProviderRegistry;
import io.jonasg.kawa.plugin.KawaPlugin;

import java.util.List;

/// The outcome of [PluginLoader#loadAll]: the registered plugins and what they contributed.
///
/// @param plugins                 the plugins, in discovery order
/// @param authenticationProviders the admin API authentication providers they registered
public record LoadedPlugins(
        List<KawaPlugin> plugins,
        AuthenticationProviderRegistry authenticationProviders
) {
}
