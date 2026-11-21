package io.jonasg.kawa.server;

import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.http.health.HealthProbe;
import io.jonasg.kawa.server.plugin.PluginLoader;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class AdminSurfaceTest {

    @Test
    void startBindsAndCloseStops() throws Exception {
        // given
        var state = new DynamicGatewayState("localhost:9092", "__kawa", new java.util.Properties());
        state.createConfigManager(new InterceptorPipeline(List.of()));
        var cache = new MetadataCache();
        var admin = new AdminSurface(
                new AdminConfig(true, "127.0.0.1", 0, null), state, cache, () -> java.util.concurrent.CompletableFuture.completedFuture(null),
                "localhost:9092", new java.util.Properties(),
                new PluginLoader(getClass().getClassLoader()).loadAll().authenticationProviders(),
                new HealthProbe() {
                    @Override
                    public boolean listenerBound() {
                        return true;
                    }

                    @Override
                    public boolean configLoaded() {
                        return true;
                    }

                    @Override
                    public Optional<Duration> timeSinceClusterRefresh() {
                        return Optional.empty();
                    }
                });

        // when
        admin.start();

        // then
        assertThat(admin.boundPort()).isGreaterThan(0);

        // when
        admin.close();
    }
}
