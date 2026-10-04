package io.jonasg.kawa.http.auth.builtin;

import io.jonasg.kawa.http.auth.none.NoneAuthenticationProviderFactory;
import io.jonasg.kawa.http.auth.oauth.OAuthAuthenticationProviderFactory;
import io.jonasg.kawa.plugin.KawaPlugin;
import io.jonasg.kawa.plugin.KawaPluginContext;

/// Registers kawa's built-in admin API authentication providers, `none` and `oauth`. Discovered
/// through `ServiceLoader` like any other plugin, so built-in and plugin providers are selected the
/// same way: by name with `admin.auth.type`.
public final class BuiltInAuthenticationPlugin implements KawaPlugin {

    @Override
    public void register(KawaPluginContext context) {
        context.registerAuthenticationProvider(new NoneAuthenticationProviderFactory());
        context.registerAuthenticationProvider(new OAuthAuthenticationProviderFactory());
    }
}
