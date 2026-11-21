package io.jonasg.kawa.http.health;

import io.jonasg.kawa.http.web.Router;

import java.util.Map;

/// `GET /health/live`: answers while the process serves HTTP. It never checks the broker, so an
/// unreachable cluster cannot make the pod restart.
final class GetHealthLiveHandler implements Router.Handler {

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return Router.Response.ok(Map.of("status", "UP"));
    }
}
