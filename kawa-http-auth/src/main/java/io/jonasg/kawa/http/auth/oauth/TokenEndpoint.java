package io.jonasg.kawa.http.auth.oauth;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.oauth2.sdk.AccessTokenResponse;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.ClientCredentialsGrant;
import com.nimbusds.oauth2.sdk.ErrorObject;
import com.nimbusds.oauth2.sdk.OAuth2Error;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.RefreshTokenGrant;
import com.nimbusds.oauth2.sdk.TokenErrorResponse;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.PlainClientSecret;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.pkce.CodeChallenge;
import com.nimbusds.oauth2.sdk.pkce.CodeChallengeMethod;
import com.nimbusds.oauth2.sdk.tokenexchange.TokenExchangeGrant;
import com.nimbusds.oauth2.sdk.token.Tokens;
import io.jonasg.kawa.plugin.auth.AdminUserDirectory;
import io.jonasg.kawa.plugin.http.HttpEndpoint;
import io.jonasg.kawa.plugin.http.HttpRequest;
import io.jonasg.kawa.plugin.http.HttpResponse;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

/// `POST /oauth/token`: the OAuth 2.0 token endpoint (RFC 6749). Answers with an access and refresh
/// token, or an RFC 6749 §5.2 error. Public: it is how a caller obtains a token in the first place.
///
/// - `grant_type=authorization_code`: redeems a single-use code from the login flow, checking the client,
///   the redirect URI and the PKCE verifier (RFC 7636), and starts a session
/// - `grant_type=refresh_token`: continues a session with a valid refresh token of a user that still
///   exists, rotating the refresh token without extending the session
/// - `grant_type=client_credentials`: the [OperatorClient] authenticates with its secret
///   (`client_secret_basic` or `client_secret_post`) and gets an access token, without a refresh token
/// - `grant_type=urn:ietf:params:oauth:grant-type:token-exchange` (RFC 8693): the operator
///   exchanges its own access token (`subject_token`) for one acting as `requested_subject`, a principal
///   its `operatorActsAs` pattern allows. `requested_subject` is a kawa parameter: RFC 8693 leaves how the
///   target principal is named to the server
///
/// The password grant is not supported (OAuth 2.1): passwords only ever go to the login endpoint.
final class TokenEndpoint implements HttpEndpoint {

    private static final URI TOKEN_ENDPOINT = URI.create("http://kawa/oauth/token");
    private static final String REQUESTED_SUBJECT = "requested_subject";

    private final AdminUserDirectory users;
    private final TokenIssuer tokenIssuer;
    private final TokenVerifier refreshTokens;
    private final AuthorizationFlow flow;
    private final OperatorClient operator;
    private final TokenVerifier accessTokens;

    TokenEndpoint(
            AdminUserDirectory users,
            TokenIssuer tokenIssuer,
            TokenVerifier refreshTokens,
            AuthorizationFlow flow,
            OperatorClient operator,
            TokenVerifier accessTokens
    ) {
        this.users = users;
        this.tokenIssuer = tokenIssuer;
        this.refreshTokens = refreshTokens;
        this.flow = flow;
        this.operator = operator;
        this.accessTokens = accessTokens;
    }

    @Override
    public HttpResponse handle(HttpRequest request) {
        TokenRequest tokenRequest;
        try {
            tokenRequest = TokenRequest.parse(toNimbusRequest(request));
        } catch (ParseException e) {
            return error(e.getErrorObject() == null ? OAuth2Error.INVALID_REQUEST : e.getErrorObject());
        }
        return switch (tokenRequest.getAuthorizationGrant()) {
            case AuthorizationCodeGrant grant -> redeem(grant, tokenRequest.getClientID());
            case RefreshTokenGrant grant -> refresh(grant);
            case ClientCredentialsGrant _ -> clientCredentials(tokenRequest.getClientAuthentication());
            case TokenExchangeGrant grant -> exchange(grant, tokenRequest.getClientAuthentication(),
                    tokenRequest.getCustomParameter(REQUESTED_SUBJECT));
            default -> error(OAuth2Error.UNSUPPORTED_GRANT_TYPE);
        };
    }

    private HttpResponse redeem(AuthorizationCodeGrant grant, ClientID clientId) {
        return flow.redeem(grant.getAuthorizationCode().getValue())
                .filter(code -> clientId != null && code.clientId().equals(clientId.getValue()))
                .filter(code -> code.redirectUri().equals(grant.getRedirectionURI()))
                .filter(code -> grant.getCodeVerifier() != null && code.codeChallenge().equals(
                        CodeChallenge.compute(CodeChallengeMethod.S256, grant.getCodeVerifier()).getValue()))
                .flatMap(code -> users.username(code.subject())
                        .map(username -> tokens(new AccessTokenResponse(tokenIssuer.newSession(code.subject(), username)))))
                .orElseGet(() -> error(OAuth2Error.INVALID_GRANT.setDescription("invalid, expired or already used authorization code")));
    }

    private HttpResponse refresh(RefreshTokenGrant grant) {
        return refreshTokens.verify(grant.getRefreshToken().getValue())
                .flatMap(claims -> users.username(claims.getSubject())
                        .map(username -> tokens(new AccessTokenResponse(
                                tokenIssuer.refreshSession(claims.getSubject(), username, sessionExpiry(claims))))))
                .orElseGet(() -> error(OAuth2Error.INVALID_GRANT.setDescription("invalid or expired refresh token")));
    }

    private HttpResponse clientCredentials(ClientAuthentication authentication) {
        if (!authenticatedOperator(authentication)) {
            return error(OAuth2Error.INVALID_CLIENT);
        }
        return tokens(new AccessTokenResponse(new Tokens(tokenIssuer.issueForOperator(), null)));
    }

    private HttpResponse exchange(TokenExchangeGrant grant, ClientAuthentication authentication, List<String> requestedSubject) {
        if (!authenticatedOperator(authentication)) {
            return error(OAuth2Error.INVALID_CLIENT);
        }
        if (requestedSubject == null || requestedSubject.size() != 1 || requestedSubject.getFirst().isBlank()) {
            return error(OAuth2Error.INVALID_REQUEST.setDescription("token exchange needs one requested_subject"));
        }
        String principal = requestedSubject.getFirst();
        boolean ownToken = accessTokens.verify(grant.getSubjectToken().getValue())
                .filter(claims -> TokenIssuer.OPERATOR_CLIENT_ID.equals(claims.getClaim(TokenIssuer.CLIENT_ID_CLAIM)))
                .filter(claims -> TokenIssuer.OPERATOR_CLIENT_ID.equals(claims.getSubject()) && claims.getClaim(TokenIssuer.ACT_CLAIM) == null)
                .isPresent();
        if (!ownToken) {
            return error(OAuth2Error.INVALID_GRANT.setDescription("subject_token is not a valid access token of the operator"));
        }
        if (!operator.mayActAs(principal)) {
            return error(OAuth2Error.UNAUTHORIZED_CLIENT.setDescription(
                    "the operator may not act as '" + principal + "' (admin.auth.settings.operatorActsAs)"));
        }
        return tokens(new AccessTokenResponse(new Tokens(tokenIssuer.issueDelegated(principal), null)));
    }

    private boolean authenticatedOperator(ClientAuthentication authentication) {
        return authentication instanceof PlainClientSecret secret
                && operator.authenticate(secret.getClientID().getValue(), secret.getClientSecret().getValue());
    }

    private static Instant sessionExpiry(JWTClaimsSet claims) {
        return claims.getExpirationTime().toInstant();
    }

    private static HttpResponse tokens(AccessTokenResponse response) {
        return noStore(HttpResponse.json(200, response.toJSONObject().toJSONString()));
    }

    private static HTTPRequest toNimbusRequest(HttpRequest request) {
        var httpRequest = new HTTPRequest(HTTPRequest.Method.POST, TOKEN_ENDPOINT);
        request.header("content-type").ifPresent(contentType -> httpRequest.setHeader("Content-Type", contentType));
        request.header("authorization").ifPresent(authorization -> httpRequest.setHeader("Authorization", authorization));
        httpRequest.setBody(new String(request.body(), StandardCharsets.UTF_8));
        return httpRequest;
    }

    private static HttpResponse error(ErrorObject error) {
        int status = error.getHTTPStatusCode() > 0 ? error.getHTTPStatusCode() : 400;
        return noStore(HttpResponse.json(status, new TokenErrorResponse(error).toJSONObject().toJSONString()));
    }

    private static HttpResponse noStore(HttpResponse response) {
        return response.withHeader("Cache-Control", "no-store").withHeader("Pragma", "no-cache");
    }
}
