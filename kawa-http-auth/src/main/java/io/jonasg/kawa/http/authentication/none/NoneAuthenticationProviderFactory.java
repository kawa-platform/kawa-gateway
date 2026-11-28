package io.jonasg.kawa.http.authentication.none;

import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// `admin.auth.type: none`, the explicit opt-out: the admin API stays open. Never the default (that is
/// `oauth`), and it warns at startup that the admin API is unauthenticated.
public final class NoneAuthenticationProviderFactory implements AuthenticationProviderFactory {

    private static final Logger log = LoggerFactory.getLogger(NoneAuthenticationProviderFactory.class);

    @Override
    public String name() {
        return "none";
    }

    @Override
    public AuthenticationProvider create(AuthenticationProviderContext context) {
        log.warn("admin.auth.type is 'none': the admin API at {} is not authenticated, so anyone who can reach it can read and "
                + "change the gateway config. Remove admin.auth.type (it defaults to 'oauth') to require a login.", context.publicUrl());
        return new AnonymousAuthenticationProvider();
    }
}
