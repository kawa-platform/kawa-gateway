package io.jonasg.kawa.http.auth.none;

import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory;

/// `admin.auth.type: none` (the default): the admin API stays open.
public final class NoneAuthenticationProviderFactory implements AuthenticationProviderFactory {

    @Override
    public String name() {
        return "none";
    }

    @Override
    public AuthenticationProvider create(AuthenticationProviderContext context) {
        return new AnonymousAuthenticationProvider();
    }
}
