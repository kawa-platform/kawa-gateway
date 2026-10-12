package io.jonasg.kawa.http.auth.oauth;

import com.nimbusds.jwt.JWTClaimsSet;
import io.jonasg.kawa.plugin.auth.AdminUserDirectory;
import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationRequest;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/// The `admin.auth.type: oauth` provider: admits requests carrying a valid access token issued by
/// kawa itself (`Authorization: Bearer <jwt>`).
///
/// A token is valid when [TokenVerifier] accepts it as an `at+jwt` access token and its `sub` is an
/// admin user that still exists and is enabled, so deleting or disabling a user revokes their tokens
/// immediately. The principal is named by the admin user's current username.
///
/// Every token names the OAuth client it was issued to in `client_id`; a token without one, or with
/// any other, is rejected. A [TokenIssuer#STUDIO_CLIENT_ID] token belongs to an admin user. An
/// [TokenIssuer#OPERATOR_CLIENT_ID] token is valid while the [OperatorClient] is still configured, and
/// the principal is the operator itself. A delegated token (with an `act` claim) is valid while the
/// operator may still act as its subject: the principal is the subject, and its actor the operator.
public final class KawaTokenAuthenticationProvider implements AuthenticationProvider {

    private static final String BEARER = "bearer ";

    private final TokenVerifier verifier;
    private final AdminUserDirectory users;
    private final OperatorClient operator;

    KawaTokenAuthenticationProvider(SigningKeys keys, String issuer, AdminUserDirectory users, OperatorClient operator) {
        this.verifier = new TokenVerifier(keys, issuer, TokenIssuer.ACCESS_TOKEN_TYPE);
        this.users = users;
        this.operator = operator;
    }

    @Override
    public Optional<KawaPrincipal> authenticate(AuthenticationRequest request) {
        return request.header("authorization")
                .filter(value -> value.length() > BEARER.length()
                        && value.substring(0, BEARER.length()).toLowerCase(Locale.ROOT).equals(BEARER))
                .map(value -> value.substring(BEARER.length()).trim())
                .flatMap(verifier::verify)
                .flatMap(this::principal);
    }

    private Optional<KawaPrincipal> principal(JWTClaimsSet claims) {
        Object clientId = claims.getClaim(TokenIssuer.CLIENT_ID_CLAIM);
        if (TokenIssuer.STUDIO_CLIENT_ID.equals(clientId)) {
            // an admin user logged in through kawa studio
            return users.username(claims.getSubject()).map(KawaPrincipal::new);
        }
        if (!TokenIssuer.OPERATOR_CLIENT_ID.equals(clientId) || !operator.enabled()) {
            return Optional.empty();
        }
        Object act = claims.getClaim(TokenIssuer.ACT_CLAIM);
        if (act == null) {
            // the operator acting as itself
            return TokenIssuer.OPERATOR_CLIENT_ID.equals(claims.getSubject())
                    ? Optional.of(new KawaPrincipal(TokenIssuer.OPERATOR_CLIENT_ID))
                    : Optional.empty();
        }
        // the operator acting as another principal
        return act instanceof Map<?, ?> actor
                && TokenIssuer.OPERATOR_CLIENT_ID.equals(actor.get("sub"))
                && operator.mayActAs(claims.getSubject())
                ? Optional.of(new KawaPrincipal(claims.getSubject(), TokenIssuer.OPERATOR_CLIENT_ID))
                : Optional.empty();
    }
}
