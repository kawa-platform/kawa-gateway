package io.jonasg.kawa.server;

import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.server.plugin.PluginLoader;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AdminSurfaceTest {

    @Test
    void startBindsAndCloseStops() throws Exception {
        // given
        var state = new DynamicGatewayState("localhost:9092", "__kawa", null);
        var cache = new MetadataCache();
        var admin = new AdminSurface(
                new AdminConfig(true, "127.0.0.1", 0, null), state, cache, "localhost:9092", null,
                new PluginLoader(getClass().getClassLoader()).loadAll().authenticationProviders());

        // when
        admin.start();

        // then
        assertThat(admin.boundPort()).isGreaterThan(0);

        // when
        admin.close();
    }
}
