package io.jonasg.kawa.http.auth.oauth;

import io.jonasg.kawa.plugin.http.HttpEndpoint;
import io.jonasg.kawa.plugin.http.HttpRequest;
import io.jonasg.kawa.plugin.http.HttpResponse;

/// `GET /.well-known/jwks.json`: the public key set that verifies kawa's admin access tokens.
/// Public, so resource servers and tools can verify tokens without calling back into kawa.
final class JwksEndpoint implements HttpEndpoint {

    private final String jwks;

    JwksEndpoint(SigningKeys keys) {
        this.jwks = keys.publicJwkSet().toString(true);
    }

    @Override
    public HttpResponse handle(HttpRequest request) {
        return HttpResponse.json(200, jwks);
    }
}
