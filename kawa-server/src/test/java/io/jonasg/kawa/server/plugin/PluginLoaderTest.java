package io.jonasg.kawa.server.plugin;

import io.jonasg.kawa.http.auth.builtin.BuiltInAuthenticationPlugin;
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
        List<KawaPlugin> plugins = loader.loadAll().plugins();

        // then
        assertThat(plugins)
                .withFailMessage(() -> "RecordingPlugin was not discovered, loaded: " + plugins)
                .hasAtLeastOneElementOfType(RecordingPlugin.class);
        RecordingPlugin recording = plugins.stream()
                .filter(RecordingPlugin.class::isInstance)
                .map(RecordingPlugin.class::cast)
                .findFirst()
                .orElseThrow();
        assertThat(recording.context())
                .withFailMessage(() -> "RecordingPlugin.register was not called with a context")
                .isNotNull();
    }

    @Test
    void loadsTheBuiltInAuthenticationProvidersAsAPlugin() {
        // given
        var loader = new PluginLoader(getClass().getClassLoader());

        // when
        var loaded = loader.loadAll();

        // then
        assertThat(loaded.plugins())
                .withFailMessage(() -> "the built-in authentication plugin was not discovered: " + loaded.plugins())
                .hasAtLeastOneElementOfType(BuiltInAuthenticationPlugin.class);
        assertThat(loaded.authenticationProviders().require("none").name()).isEqualTo("none");
        assertThat(loaded.authenticationProviders().require("oauth").name()).isEqualTo("oauth");
    }
}
