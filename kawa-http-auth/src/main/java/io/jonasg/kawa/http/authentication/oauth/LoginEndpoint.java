package io.jonasg.kawa.http.authentication.oauth;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationSuccessResponse;
import com.nimbusds.oauth2.sdk.ResponseMode;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.jwt.JWTClaimsSet;
import io.jonasg.kawa.plugin.auth.AdminUserDirectory;
import io.jonasg.kawa.plugin.http.HttpEndpoint;
import io.jonasg.kawa.plugin.http.HttpRequest;
import io.jonasg.kawa.plugin.http.HttpResponse;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/// `POST /oauth/login`: completes the login step of the authorization code flow for the external login
/// UI. The UI posts `{"login_challenge", "username", "password"}` as JSON, or, for an admin user already
/// signed in to the UI, `{"login_challenge"}` alone with their admin access token as `Authorization: Bearer`
/// ("continue as"): the token's user, if still enabled, is then the one signing in. Only an access token for
/// the admin API counts, never one meant for the MCP endpoint. On success kawa answers
/// `{"redirect_to": "<redirect_uri>?code=…&state=…"}` and the UI sends the browser there, back to the
/// client. Errors are `400`s with `invalid_request`, `invalid_challenge` (expired or tampered with:
/// start the flow again) or `invalid_credentials`.
final class LoginEndpoint implements HttpEndpoint {

    private static final String BEARER = "bearer ";

    private final AuthorizationFlow flow;
    private final AdminUserDirectory users;
    private final TokenVerifier adminAccessTokens;

    LoginEndpoint(AuthorizationFlow flow, AdminUserDirectory users, TokenVerifier adminAccessTokens) {
        this.flow = flow;
        this.users = users;
        this.adminAccessTokens = adminAccessTokens;
    }

    @Override
    public HttpResponse handle(HttpRequest request) {
        Map<String, Object> body;
        try {
            body = JSONObjectUtils.parse(new String(request.body(), StandardCharsets.UTF_8));
        } catch (java.text.ParseException e) {
            return error("invalid_request", "the body must be a JSON object");
        }
        Optional<String> challenge = string(body, "login_challenge");
        Optional<String> username = string(body, "username");
        Optional<String> password = string(body, "password");
        boolean withPassword = username.isPresent() || password.isPresent();
        if (challenge.isEmpty() || (withPassword && (username.isEmpty() || password.isEmpty()))) {
            return error("invalid_request", "login_challenge, and username with password, are required");
        }
        var pending = flow.pending(challenge.get());
        if (pending.isEmpty()) {
            return error("invalid_challenge", "the login request has expired, start again");
        }
        Optional<String> subject = withPassword
                ? users.authenticate(username.get(), password.get())
                : signedInSubject(request);
        if (subject.isEmpty()) {
            return error("invalid_credentials", withPassword ? "invalid username or password" : "not signed in");
        }
        var authorization = pending.get();
        String code = flow.code(subject.get(), authorization);
        String redirectTo = new AuthorizationSuccessResponse(authorization.redirectUri(), new AuthorizationCode(code), null,
                authorization.state() == null ? null : new State(authorization.state()), ResponseMode.QUERY).toURI().toString();
        return noStore(HttpResponse.json(200, JSONObjectUtils.toJSONString(Map.of("redirect_to", redirectTo))));
    }

    /// The subject of the enabled admin user whose admin access token the request carries.
    private Optional<String> signedInSubject(HttpRequest request) {
        return request.header("authorization")
                .filter(value -> value.length() > BEARER.length()
                        && value.substring(0, BEARER.length()).toLowerCase(Locale.ROOT).equals(BEARER))
                .map(value -> value.substring(BEARER.length()).trim())
                .flatMap(adminAccessTokens::verify)
                .map(JWTClaimsSet::getSubject)
                .filter(subject -> users.username(subject).isPresent());
    }

    private static Optional<String> string(Map<String, Object> body, String name) {
        return body.get(name) instanceof String value && !value.isBlank() ? Optional.of(value) : Optional.empty();
    }

    private static HttpResponse error(String error, String description) {
        return noStore(HttpResponse.json(400,
                JSONObjectUtils.toJSONString(Map.of("error", error, "error_description", description))));
    }

    private static HttpResponse noStore(HttpResponse response) {
        return response.withHeader("Cache-Control", "no-store");
    }
}
