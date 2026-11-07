package io.jonasg.kawa.http.authentication.oauth;

import com.nimbusds.jose.util.JSONObjectUtils;
import io.jonasg.kawa.plugin.http.HttpEndpoint;
import io.jonasg.kawa.plugin.http.HttpRequest;
import io.jonasg.kawa.plugin.http.HttpResponse;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/// `GET /oauth/login-challenge?login_challenge=…`: who is asking, for the external login UI to show before the user
/// enters credentials. The challenge is a signed token, so kawa verifies it and answers with what it carries; the UI
/// never decodes it itself. The answer is `{"clientId", "clientName", "redirectHost", "audience", "registered"}`:
/// `clientName` is the client's display name (a metadata document's `client_name`, else the id), `redirectHost` the
/// host the browser returns to, `audience` `mcp` or `admin` (what the client's tokens are accepted for) and
/// `registered` whether an admin registered the client (`false` for a client that identifies itself with a metadata
/// document). A missing, expired or tampered challenge is a `400` `invalid_challenge`.
final class LoginChallengeEndpoint implements HttpEndpoint {

    private final AuthorizationFlow flow;
    private final String mcpAudience;

    LoginChallengeEndpoint(AuthorizationFlow flow, String mcpAudience) {
        this.flow = flow;
        this.mcpAudience = mcpAudience;
    }

    @Override
    public HttpResponse handle(HttpRequest request) {
        String challenge = request.queryParams().get("login_challenge");
        Optional<AuthorizationFlow.PendingAuthorization> pending = challenge == null || challenge.isBlank()
                ? Optional.empty()
                : flow.pending(URLDecoder.decode(challenge, StandardCharsets.UTF_8));
        if (pending.isEmpty()) {
            return noStore(HttpResponse.json(400, JSONObjectUtils.toJSONString(Map.of(
                    "error", "invalid_challenge", "error_description", "the login request has expired, start again"))));
        }
        var found = pending.get();
        var body = new LinkedHashMap<String, Object>();
        body.put("clientId", found.clientId());
        body.put("clientName", found.clientName());
        body.put("redirectHost", found.redirectUri().getHost());
        body.put("audience", found.audience().equals(mcpAudience) ? "mcp" : "admin");
        body.put("registered", found.registered());
        return noStore(HttpResponse.json(200, JSONObjectUtils.toJSONString(body)));
    }

    private static HttpResponse noStore(HttpResponse response) {
        return response.withHeader("Cache-Control", "no-store");
    }
}
