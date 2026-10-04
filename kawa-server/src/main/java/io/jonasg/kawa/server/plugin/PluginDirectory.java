package io.jonasg.kawa.server.plugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

/// Where external plugins are installed: `--plugins <dir>`, else the `KAWA_PLUGIN_DIR` environment
/// variable, else [#DEFAULT] when it exists. A directory that is configured explicitly must exist.
public final class PluginDirectory {

    /// The plugin directory of the kawa Docker image.
    public static final Path DEFAULT = Path.of("/opt/kawa/plugins");

    static final String ENV = "KAWA_PLUGIN_DIR";

    private PluginDirectory() {
    }

    public static Optional<Path> resolve(String[] args, Map<String, String> env) {
        return resolve(args, env, DEFAULT);
    }

    static Optional<Path> resolve(String[] args, Map<String, String> env, Path defaultDirectory) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("--plugins")) {
                return Optional.of(existing(Path.of(args[i + 1]), "--plugins"));
            }
        }
        String fromEnv = env.get(ENV);
        if (fromEnv != null && !fromEnv.isBlank()) {
            return Optional.of(existing(Path.of(fromEnv), ENV));
        }
        return Files.isDirectory(defaultDirectory) ? Optional.of(defaultDirectory) : Optional.empty();
    }

    private static Path existing(Path directory, String source) {
        if (!Files.isDirectory(directory)) {
            throw new IllegalArgumentException("plugin directory " + directory + " (" + source + ") does not exist");
        }
        return directory;
    }
}
