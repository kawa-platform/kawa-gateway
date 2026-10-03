package io.jonasg.kawa.server.plugin;

import io.jonasg.kawa.plugin.KawaPlugin;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PluginLoaderTest {

    @Test
    void discoversAndRegistersPluginsOnTheClasspath() {
        // given
        var loader = new PluginLoader(getClass().getClassLoader());

        // when
        List<KawaPlugin> plugins = loader.loadAll();

        // then
        assertThat(plugins)
                .withFailMessage(() -> "RecordingPlugin was not discovered, loaded: " + plugins)
                .singleElement()
                .isInstanceOf(RecordingPlugin.class);
        assertThat(((RecordingPlugin) plugins.getFirst()).context())
                .withFailMessage(() -> "RecordingPlugin.register was not called with a context")
                .isNotNull();
    }
}
