package io.jonasg.kawa.http.authentication.oauth;

import com.nimbusds.jwt.JWTClaimsSet;
import io.jonasg.kawa.plugin.auth.AdminUserDirectory;
import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationRequest;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/// The `admin.auth.type: oauth` provider: admits requests carrying a valid access token issued by
/// kawa itself (`Authorization: Bearer <jwt>`).
///
/// A token is valid when [TokenVerifier] accepts it as an `at+jwt` access token for one of the accepted
/// audiences, the OAuth client it was issued to may still use it ([OAuthClients#clientOf]) and its `sub` is an admin
/// user that still exists and is enabled, so disabling or deleting the client or the user revokes their tokens
/// immediately. A token for the MCP endpoint (`aud = <publicUrl>/mcp`) is accepted only on `POST /mcp`; anywhere else
/// the request is unauthenticated. The principal is named by the admin user's current username and carries the
/// token's client.
///
/// Every token names the OAuth client it was issued to in `client_id`; a token without one is
/// rejected. A token issued to any client but the operator (kawa studio or a stored OAuth client)
/// belongs to an admin user. An
/// [TokenIssuer#OPERATOR_CLIENT_ID] token is valid while the [OperatorClient] is still configured, and
/// the principal is the operator itself: named `kawa-operator` and with `kawa-operator` as its actor. A
/// delegated token (with an `act` claim) is valid while the operator may still act as its subject: the
/// principal is the subject, and its actor the operator. The actor is therefore `kawa-operator` exactly
/// when the operator made the request, which a name cannot fake: an admin user may be called
/// `kawa-operator` too, but its principal never carries an actor.
///
/// A rejected request is challenged with the URL of the RFC 9728 protected resource metadata, from
/// which a client can discover kawa's authorization server.
public final class KawaTokenAuthenticationProvider implements AuthenticationProvider {

    private static final String BEARER = "bearer ";

    private final TokenVerifier verifier;
    private final OAuthClients clients;
    private final AdminUserDirectory users;
    private final OperatorClient operator;
    private final String challenge;

    /// @param audiences the `aud` values a token may carry
    /// @param publicUrl the admin API's public URL, under which the protected resource metadata is served
    KawaTokenAuthenticationProvider(SigningKeys keys, String issuer, Set<String> audiences, OAuthClients clients,
                                    AdminUserDirectory users, OperatorClient operator, String publicUrl) {
        this.verifier = new TokenVerifier(keys, issuer, audiences, TokenIssuer.ACCESS_TOKEN_TYPE);
        this.clients = clients;
        this.users = users;
        this.operator = operator;
        this.challenge = "Bearer resource_metadata=\"" + publicUrl + ProtectedResourceMetadataEndpoint.PATH + "\"";
    }

    @Override
    public Optional<KawaPrincipal> authenticate(AuthenticationRequest request) {
        return request.header("authorization")
                .filter(value -> value.length() > BEARER.length()
                        && value.substring(0, BEARER.length()).toLowerCase(Locale.ROOT).equals(BEARER))
                .map(value -> value.substring(BEARER.length()).trim())
                .flatMap(verifier::verify)
                .filter(token -> isMeantFor(token, request))
                .flatMap(this::principal);
    }

    /// Whether `token` may be used for `request`: a token for the MCP endpoint only on exactly `POST /mcp`, any
    /// other token anywhere. Which kind a token is follows from its `aud`, never from the request.
    private boolean isMeantFor(JWTClaimsSet token, AuthenticationRequest request) {
        return !token.getAudience().contains(clients.mcpAudience())
                || "POST".equals(request.method()) && OAuthAuthenticationProviderFactory.MCP_PATH.equals(request.path());
    }

    private Optional<KawaPrincipal> principal(JWTClaimsSet claims) {
        Object clientId = claims.getClaim(TokenIssuer.CLIENT_ID_CLAIM);
        if (TokenIssuer.OPERATOR_CLIENT_ID.equals(clientId)) {
            return operatorPrincipal(claims);
        }
        if (clientId == null) {
            return Optional.empty();
        }
        // an admin user logged in through kawa studio or a stored OAuth client, which must still be enabled
        return clients.clientOf(claims).flatMap(client -> users.username(claims.getSubject())
                .map(username -> new KawaPrincipal(username, client.id())));
    }

    private Optional<KawaPrincipal> operatorPrincipal(JWTClaimsSet claims) {
        if (!operator.enabled()) {
            return Optional.empty();
        }
        Object act = claims.getClaim(TokenIssuer.ACT_CLAIM);
        if (act == null) {
            // the operator acting as itself
            return TokenIssuer.OPERATOR_CLIENT_ID.equals(claims.getSubject())
                    ? Optional.of(new KawaPrincipal(TokenIssuer.OPERATOR_CLIENT_ID, TokenIssuer.OPERATOR_CLIENT_ID))
                    : Optional.empty();
        }
        // the operator acting as another principal
        return act instanceof Map<?, ?> actor
                && TokenIssuer.OPERATOR_CLIENT_ID.equals(actor.get("sub"))
                && operator.mayActAs(claims.getSubject())
                ? Optional.of(new KawaPrincipal(claims.getSubject(), TokenIssuer.OPERATOR_CLIENT_ID))
                : Optional.empty();
    }

    @Override
    public String challenge() {
        return challenge;
    }
}
