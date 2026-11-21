package io.jonasg.kawa.http.health;

import io.jonasg.kawa.http.web.Router;

/// Registers the unauthenticated `/health/live` and `/health/ready` probes on a [Router].
public final class HealthRoutes {

    private HealthRoutes() {
    }

    public static void register(Router router, HealthProbe probe) {
        router.publicGet("/health/live", new GetHealthLiveHandler())
                .publicGet("/health/ready", new GetHealthReadyHandler(probe));
    }
}
