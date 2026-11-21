package io.jonasg.kawa.server;

import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.http.health.HealthProbe;
import io.jonasg.kawa.server.netty.KafkaListener;

import java.time.Duration;
import java.util.Optional;

/// The gateway's readiness inputs, read live from the components on every health request.
final class GatewayHealth implements HealthProbe {

    private final KafkaListener listener;
    private final DynamicGatewayState state;
    private final MetadataCache cache;

    GatewayHealth(KafkaListener listener, DynamicGatewayState state, MetadataCache cache) {
        this.listener = listener;
        this.state = state;
        this.cache = cache;
    }

    @Override
    public boolean listenerBound() {
        return listener.boundPort() > 0;
    }

    @Override
    public boolean configLoaded() {
        return state.isConfigLoaded();
    }

    @Override
    public Optional<Duration> timeSinceClusterRefresh() {
        return cache.timeSinceLastRefresh();
    }
}
