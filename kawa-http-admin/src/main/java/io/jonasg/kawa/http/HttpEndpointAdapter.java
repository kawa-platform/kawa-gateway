package io.jonasg.kawa.http;

import io.jonasg.kawa.plugin.http.HttpEndpoint;
import io.jonasg.kawa.plugin.http.HttpRequest;
import io.jonasg.kawa.plugin.http.HttpResponse;

import java.nio.charset.StandardCharsets;

/// Serves a plugin's [HttpEndpoint] as a [Router.Handler], translating between the router's request
/// and response and the plugin API's.
final class HttpEndpointAdapter implements Router.Handler {

    private final HttpEndpoint endpoint;

    HttpEndpointAdapter(HttpEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        HttpResponse response = endpoint.handle(new HttpRequest(
                request.method(), request.path(), request.queryParams(), request.headers(), request.body()));
        String body = new String(response.body(), StandardCharsets.UTF_8);
        return new Router.Response<>(response.status(), body, response.contentType(), true, response.headers());
    }
}
