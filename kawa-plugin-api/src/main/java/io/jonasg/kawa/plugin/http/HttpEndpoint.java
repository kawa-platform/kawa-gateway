package io.jonasg.kawa.plugin.http;

/// A plugin-provided admin HTTP endpoint. Called off the network event loop; must be thread-safe.
@FunctionalInterface
public interface HttpEndpoint {

    HttpResponse handle(HttpRequest request);
}
