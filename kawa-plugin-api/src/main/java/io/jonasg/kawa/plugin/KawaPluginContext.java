package io.jonasg.kawa.plugin;

import io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory;

/// What the gateway hands to [KawaPlugin#register]: the extension points a plugin can contribute to.
public interface KawaPluginContext {

    /// Makes an admin API authentication provider available under [AuthenticationProviderFactory#name],
    /// selectable with `admin.auth.type`. Registering a name that is already taken aborts startup.
    void registerAuthenticationProvider(AuthenticationProviderFactory factory);
}
