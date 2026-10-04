package io.jonasg.kawa.server.plugin;

import io.jonasg.kawa.plugin.KawaPlugin;

import javax.tools.ToolProvider;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

/// Builds plugin JARs for tests: compiles Java sources against the real plugin API and packs the result.
final class PluginJars {

    private PluginJars() {
    }

    /// Compiles `sources` (class name → source) and writes the classes, except `excluded`, to `jar`. When
    /// `pluginClass` is set the JAR registers it in `META-INF/services/io.jonasg.kawa.plugin.KawaPlugin`.
    static Path build(Path jar, Map<String, String> sources, Set<String> excluded, String pluginClass) throws IOException {
        Path work = Files.createTempDirectory("plugin-src");
        Path classes = work.resolve("classes");
        Files.createDirectories(classes);
        List<String> args = new ArrayList<>(List.of("-d", classes.toString(), "-classpath", pluginApiLocation()));
        for (var source : sources.entrySet()) {
            Path file = work.resolve("src").resolve(source.getKey().replace('.', '/') + ".java");
            Files.createDirectories(file.getParent());
            Files.writeString(file, source.getValue());
            args.add(file.toString());
        }
        int exit = ToolProvider.getSystemJavaCompiler().run(null, null, null, args.toArray(String[]::new));
        if (exit != 0) {
            throw new IllegalStateException("fixture compilation failed");
        }
        Files.createDirectories(jar.getParent());
        try (OutputStream out = Files.newOutputStream(jar); var jarOut = new JarOutputStream(out);
             Stream<Path> files = Files.walk(classes)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                String entry = classes.relativize(file).toString().replace('\\', '/');
                String className = entry.substring(0, entry.length() - ".class".length()).replace('/', '.');
                if (excluded.contains(className.replaceAll("\\$.*", ""))) {
                    continue;
                }
                jarOut.putNextEntry(new JarEntry(entry));
                jarOut.write(Files.readAllBytes(file));
                jarOut.closeEntry();
            }
            if (pluginClass != null) {
                jarOut.putNextEntry(new JarEntry("META-INF/services/" + KawaPlugin.class.getName()));
                jarOut.write(pluginClass.getBytes(StandardCharsets.UTF_8));
                jarOut.closeEntry();
            }
        }
        return jar;
    }

    private static String pluginApiLocation() {
        try {
            return Path.of(KawaPlugin.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }
}
