package io.jonasg.kawa.http.auth.oauth;

import com.nimbusds.oauth2.sdk.pkce.CodeChallenge;
import com.nimbusds.oauth2.sdk.pkce.CodeChallengeMethod;
import com.nimbusds.oauth2.sdk.pkce.CodeVerifier;
import io.jonasg.kawa.plugin.http.HttpRequest;
import io.jonasg.kawa.plugin.http.HttpResponse;

import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/// Drives the authorization code flow with PKCE through a provider's registered endpoints, the way
/// kawa studio and the browser do: authorize → login UI (challenge) → login → code → token.
final class AuthorizationCodeFlow {

    static final String CLIENT_ID = "kawa-studio";
    static final String REDIRECT_URI = "http://localhost:5173/admin/auth/callback";
    static final String LOGIN_URL = "http://localhost:5173/admin/login";

    private final FakeProviderContext context;
    final CodeVerifier verifier = new CodeVerifier();

    AuthorizationCodeFlow(FakeProviderContext context) {
        this.context = context;
    }

    /// The full flow: the token endpoint's response for `username` / `password`.
    HttpResponse signIn(String username, String password) {
        return exchange(code(login(challenge(authorize(defaultAuthorization())), username, password)), REDIRECT_URI);
    }

    /// `GET /oauth/authorize` with the default, valid parameters.
    Map<String, String> defaultAuthorization() {
        var params = new LinkedHashMap<String, String>();
        params.put("response_type", "code");
        params.put("client_id", CLIENT_ID);
        params.put("redirect_uri", REDIRECT_URI);
        params.put("code_challenge", CodeChallenge.compute(CodeChallengeMethod.S256, verifier).getValue());
        params.put("code_challenge_method", "S256");
        params.put("state", "xyz");
        return params;
    }

    HttpResponse authorize(Map<String, String> params) {
        // kawa hands endpoints the query parameter values still URL-encoded
        Map<String, String> encoded = params.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey, entry -> URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8)));
        return endpoint("GET /oauth/authorize").handle(new HttpRequest("GET", "/oauth/authorize", encoded, Map.of(), new byte[0]));
    }

    /// The `login_challenge` from the authorize redirect to the login UI.
    static String challenge(HttpResponse authorizeRedirect) {
        return queryParam(authorizeRedirect.headers().get("Location"), "login_challenge");
    }

    HttpResponse login(String challenge, String username, String password) {
        String json = "{\"login_challenge\":\"" + challenge + "\",\"username\":\"" + username + "\",\"password\":\"" + password + "\"}";
        return endpoint("POST /oauth/login").handle(new HttpRequest("POST", "/oauth/login", Map.of(),
                Map.of("content-type", List.of("application/json")), json.getBytes(StandardCharsets.UTF_8)));
    }

    /// The authorization code from the login response's `redirect_to`.
    static String code(HttpResponse loginResponse) {
        String body = new String(loginResponse.body(), StandardCharsets.UTF_8);
        String redirectTo = body.replaceAll(".*\"redirect_to\":\"([^\"]*)\".*", "$1").replace("\\/", "/");
        return queryParam(redirectTo, "code");
    }

    HttpResponse exchange(String code, String redirectUri) {
        return token("grant_type=authorization_code&client_id=" + CLIENT_ID
                + "&code=" + URLEncoder.encode(code, StandardCharsets.UTF_8)
                + "&redirect_uri=" + URLEncoder.encode(redirectUri, StandardCharsets.UTF_8)
                + "&code_verifier=" + verifier.getValue());
    }

    HttpResponse token(String form) {
        return endpoint("POST /oauth/token").handle(new HttpRequest("POST", "/oauth/token", Map.of(),
                Map.of("content-type", List.of("application/x-www-form-urlencoded")), form.getBytes(StandardCharsets.UTF_8)));
    }

    static String queryParam(String url, String name) {
        return Arrays.stream(URI.create(url).getRawQuery().split("&"))
                .map(pair -> pair.split("=", 2))
                .filter(pair -> pair[0].equals(name))
                .map(pair -> URLDecoder.decode(pair[1], StandardCharsets.UTF_8))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no " + name + " in " + url));
    }

    private io.jonasg.kawa.plugin.http.HttpEndpoint endpoint(String route) {
        var endpoint = context.endpoints.get(route);
        if (endpoint == null) {
            throw new AssertionError(route + " was not registered");
        }
        return endpoint;
    }
}
