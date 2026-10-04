package io.jonasg.kawa.http.auth.oauth;

import com.nimbusds.oauth2.sdk.AuthorizationErrorResponse;
import com.nimbusds.oauth2.sdk.AuthorizationRequest;
import com.nimbusds.oauth2.sdk.ErrorObject;
import com.nimbusds.oauth2.sdk.OAuth2Error;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.ResponseMode;
import com.nimbusds.oauth2.sdk.ResponseType;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.oauth2.sdk.pkce.CodeChallengeMethod;
import io.jonasg.kawa.plugin.http.HttpEndpoint;
import io.jonasg.kawa.plugin.http.HttpRequest;
import io.jonasg.kawa.plugin.http.HttpResponse;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;

/// `GET /oauth/authorize`: the authorization endpoint (RFC 6749 §4.1, with PKCE per RFC 7636). kawa
/// serves no login page itself: a valid request is redirected to the external login UI (`loginUrl`,
/// part of kawa studio) with a `login_challenge` that carries the request.
///
/// An unknown client or redirect URI is answered with a `400` and never redirected (RFC 6749 §4.1.2.1);
/// any other problem, such as a missing PKCE challenge, is reported back to the client's redirect URI.
final class AuthorizeEndpoint implements HttpEndpoint {

    private static final String SELF = "http://kawa/oauth/authorize";

    private final AuthorizationFlow flow;
    private final String clientId;
    private final Set<URI> redirectUris;
    private final String loginUrl;

    AuthorizeEndpoint(AuthorizationFlow flow, String clientId, Set<URI> redirectUris, String loginUrl) {
        this.flow = flow;
        this.clientId = clientId;
        this.redirectUris = redirectUris;
        this.loginUrl = loginUrl;
    }

    @Override
    public HttpResponse handle(HttpRequest request) {
        AuthorizationRequest authorization;
        try {
            authorization = AuthorizationRequest.parse(URI.create(SELF + "?" + query(request)));
        } catch (ParseException e) {
            if (isRegistered(e.getClientID() == null ? null : e.getClientID().getValue(), e.getRedirectionURI())) {
                return redirectError(e.getRedirectionURI(), e.getErrorObject(), e.getState());
            }
            return badRequest(e.getMessage());
        }
        URI redirectUri = authorization.getRedirectionURI();
        if (!isRegistered(authorization.getClientID().getValue(), redirectUri)) {
            return badRequest("unknown client_id or redirect_uri");
        }
        if (!authorization.getResponseType().equals(ResponseType.CODE)) {
            return redirectError(redirectUri, OAuth2Error.UNSUPPORTED_RESPONSE_TYPE, authorization.getState());
        }
        if (authorization.getCodeChallenge() == null
                || !CodeChallengeMethod.S256.equals(authorization.getCodeChallengeMethod())) {
            return redirectError(redirectUri, OAuth2Error.INVALID_REQUEST.setDescription("PKCE with S256 is required"),
                    authorization.getState());
        }
        String challenge = flow.challenge(new AuthorizationFlow.PendingAuthorization(
                clientId, redirectUri, authorization.getCodeChallenge().getValue(),
                authorization.getState() == null ? null : authorization.getState().getValue()));
        String separator = loginUrl.contains("?") ? "&" : "?";
        return HttpResponse.redirect(loginUrl + separator + "login_challenge="
                + URLEncoder.encode(challenge, StandardCharsets.UTF_8));
    }

    private boolean isRegistered(String requestedClientId, URI requestedRedirectUri) {
        return clientId.equals(requestedClientId) && requestedRedirectUri != null && redirectUris.contains(requestedRedirectUri);
    }

    /// The query string as received: kawa hands over parameter values still URL-encoded.
    private static String query(HttpRequest request) {
        return request.queryParams().entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));
    }

    private static HttpResponse redirectError(URI redirectUri, ErrorObject error, State state) {
        return HttpResponse.redirect(new AuthorizationErrorResponse(redirectUri, error, state, ResponseMode.QUERY)
                .toURI().toString());
    }

    private static HttpResponse badRequest(String description) {
        return HttpResponse.json(400, OAuth2Error.INVALID_REQUEST.setDescription(description).toJSONObject().toJSONString());
    }
}
