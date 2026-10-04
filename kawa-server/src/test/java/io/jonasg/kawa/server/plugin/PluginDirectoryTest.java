package io.jonasg.kawa.server.plugin;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PluginDirectoryTest {

    @TempDir
    Path tmp;

    @Test
    void prefersTheCommandLineOverTheEnvironmentAndTheDefault() throws IOException {
        // given
        Path fromArgs = Files.createDirectories(tmp.resolve("args"));
        Path fromEnv = Files.createDirectories(tmp.resolve("env"));

        // when
        var resolved = PluginDirectory.resolve(new String[] {"--config", "c.yaml", "--plugins", fromArgs.toString()},
                Map.of(PluginDirectory.ENV, fromEnv.toString()), tmp);

        // then
        assertThat(resolved).hasValue(fromArgs);
    }

    @Test
    void prefersTheEnvironmentOverTheDefault() throws IOException {
        // given
        Path fromEnv = Files.createDirectories(tmp.resolve("env"));

        // when
        var resolved = PluginDirectory.resolve(new String[0], Map.of(PluginDirectory.ENV, fromEnv.toString()), tmp);

        // then
        assertThat(resolved).hasValue(fromEnv);
    }

    @Test
    void usesTheDefaultOnlyWhenItExists() {
        assertThat(PluginDirectory.resolve(new String[0], Map.of(), tmp)).hasValue(tmp);
        assertThat(PluginDirectory.resolve(new String[0], Map.of(), tmp.resolve("absent"))).isEmpty();
    }

    @Test
    void rejectsAConfiguredDirectoryThatDoesNotExist() {
        assertThatThrownBy(() -> PluginDirectory.resolve(new String[] {"--plugins", tmp.resolve("absent").toString()},
                Map.of(), tmp))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not exist");
    }
}
