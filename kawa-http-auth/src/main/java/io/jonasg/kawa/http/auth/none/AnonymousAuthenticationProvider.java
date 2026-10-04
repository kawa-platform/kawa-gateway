package io.jonasg.kawa.http.auth.none;

import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationRequest;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;

import java.util.Optional;

/// The `admin.auth.type: none` provider: admits every request as the placeholder principal `admin`,
/// which is how the admin API behaved before authentication existed.
public final class AnonymousAuthenticationProvider implements AuthenticationProvider {

    private static final KawaPrincipal ADMIN = new KawaPrincipal("admin");

    @Override
    public Optional<KawaPrincipal> authenticate(AuthenticationRequest request) {
        return Optional.of(ADMIN);
    }
}
