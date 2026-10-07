package io.jonasg.kawa.http.auth.oauth;

import com.nimbusds.jwt.JWTClaimsSet;
import io.jonasg.kawa.plugin.auth.AdminUserDirectory;
import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationRequest;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;

import java.util.Locale;
import java.util.Optional;

/// The `admin.auth.type: oauth` provider: admits requests carrying a valid access token issued by
/// kawa itself (`Authorization: Bearer <jwt>`).
///
/// A token is valid when [TokenVerifier] accepts it as an `at+jwt` access token and its `sub` is an
/// admin user that still exists and is enabled, so deleting or disabling a user revokes their tokens
/// immediately. The principal is named by the admin user's current username.
public final class KawaTokenAuthenticationProvider implements AuthenticationProvider {

    private static final String BEARER = "bearer ";

    private final TokenVerifier verifier;
    private final AdminUserDirectory users;

    public KawaTokenAuthenticationProvider(SigningKeys keys, String issuer, AdminUserDirectory users) {
        this.verifier = new TokenVerifier(keys, issuer, TokenIssuer.ACCESS_TOKEN_TYPE);
        this.users = users;
    }

    @Override
    public Optional<KawaPrincipal> authenticate(AuthenticationRequest request) {
        return request.header("authorization")
                .filter(value -> value.length() > BEARER.length()
                        && value.substring(0, BEARER.length()).toLowerCase(Locale.ROOT).equals(BEARER))
                .map(value -> value.substring(BEARER.length()).trim())
                .flatMap(verifier::verify)
                .map(JWTClaimsSet::getSubject)
                .flatMap(users::username)
                .map(KawaPrincipal::new);
    }
}
