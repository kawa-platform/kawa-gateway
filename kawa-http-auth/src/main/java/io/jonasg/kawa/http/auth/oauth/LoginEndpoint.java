package io.jonasg.kawa.http.auth.oauth;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationSuccessResponse;
import com.nimbusds.oauth2.sdk.ResponseMode;
import com.nimbusds.oauth2.sdk.id.State;
import io.jonasg.kawa.plugin.auth.AdminUserDirectory;
import io.jonasg.kawa.plugin.http.HttpEndpoint;
import io.jonasg.kawa.plugin.http.HttpRequest;
import io.jonasg.kawa.plugin.http.HttpResponse;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/// `POST /oauth/login`: completes the login step of the authorization code flow for the external login
/// UI. The UI posts `{"login_challenge", "username", "password"}` as JSON; on success kawa answers
/// `{"redirect_to": "<redirect_uri>?code=…&state=…"}` and the UI sends the browser there, back to the
/// client. Errors are `400`s with `invalid_request`, `invalid_challenge` (expired or tampered with:
/// start the flow again) or `invalid_credentials`.
final class LoginEndpoint implements HttpEndpoint {

    private final AuthorizationFlow flow;
    private final AdminUserDirectory users;

    LoginEndpoint(AuthorizationFlow flow, AdminUserDirectory users) {
        this.flow = flow;
        this.users = users;
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
        if (challenge.isEmpty() || username.isEmpty() || password.isEmpty()) {
            return error("invalid_request", "login_challenge, username and password are required");
        }
        var pending = flow.pending(challenge.get());
        if (pending.isEmpty()) {
            return error("invalid_challenge", "the login request has expired, start again");
        }
        if (!users.verify(username.get(), password.get())) {
            return error("invalid_credentials", "invalid username or password");
        }
        var authorization = pending.get();
        String code = flow.code(username.get(), authorization);
        String redirectTo = new AuthorizationSuccessResponse(authorization.redirectUri(), new AuthorizationCode(code), null,
                authorization.state() == null ? null : new State(authorization.state()), ResponseMode.QUERY).toURI().toString();
        return noStore(HttpResponse.json(200, JSONObjectUtils.toJSONString(Map.of("redirect_to", redirectTo))));
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
