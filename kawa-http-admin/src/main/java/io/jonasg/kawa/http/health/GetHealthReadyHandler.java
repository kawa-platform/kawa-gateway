package io.jonasg.kawa.http.health;

import io.jonasg.kawa.http.web.Router;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/// `GET /health/ready`: `200` while the gateway can serve clients, otherwise `503` naming each
/// condition that fails. The cluster view counts as stale after three metadata refreshes (10 s each).
final class GetHealthReadyHandler implements Router.Handler {

    static final Duration MAX_CLUSTER_VIEW_AGE = Duration.ofSeconds(30);

    private final HealthProbe probe;

    GetHealthReadyHandler(HealthProbe probe) {
        this.probe = probe;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        List<Failure> failures = failures();
        if (failures.isEmpty()) {
            return Router.Response.ok(new ReadyView("UP", List.of()));
        }
        return new Router.Response<>(503, new ReadyView("DOWN", failures), "application/json", false);
    }

    private List<Failure> failures() {
        var failures = new ArrayList<Failure>();
        if (!probe.listenerBound()) {
            failures.add(new Failure("listener", "the Kafka listener is not bound"));
        }
        if (!probe.configLoaded()) {
            failures.add(new Failure("config", "the dynamic config has not been loaded from the config topic"));
        }
        Optional<Duration> age = probe.timeSinceClusterRefresh();
        if (age.isEmpty()) {
            failures.add(new Failure("clusterView", "the cluster view has not been refreshed from the brokers yet"));
        } else if (age.get().compareTo(MAX_CLUSTER_VIEW_AGE) > 0) {
            failures.add(new Failure("clusterView", "the cluster view is stale: last successful refresh "
                    + age.get().toSeconds() + " s ago"));
        }
        return failures;
    }

    record ReadyView(String status, List<Failure> failures) {
    }

    record Failure(String condition, String detail) {
    }
}
