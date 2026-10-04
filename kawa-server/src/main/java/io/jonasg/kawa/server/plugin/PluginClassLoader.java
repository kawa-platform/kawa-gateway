package io.jonasg.kawa.server.plugin;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.List;

/// The classloader of one external plugin: its own JARs on top of the JDK, isolated from kawa's
/// classpath.
///
/// Its parent is the platform classloader, so a plugin sees the JDK and its own JARs, and never kawa's
/// internals or the libraries kawa bundles (Netty, Jackson, Nimbus, …). A plugin can therefore ship any
/// version of any library without clashing with kawa. The only exceptions are the [#SHARED_PACKAGES]:
/// the plugin API, whose classes must be identical on both sides for kawa to call the plugin, and
/// slf4j, so plugin logs go through kawa's logging.
final class PluginClassLoader extends URLClassLoader {

    /// Package prefixes a plugin resolves from kawa rather than from its own JARs.
    static final List<String> SHARED_PACKAGES = List.of("io.jonasg.kawa.plugin.", "org.slf4j.");

    private final ClassLoader kawa;
    private final Path location;

    PluginClassLoader(Path location, List<URL> jars, ClassLoader kawa) {
        super("kawa-plugin:" + location.getFileName(), jars.toArray(URL[]::new), ClassLoader.getPlatformClassLoader());
        this.kawa = kawa;
        this.location = location;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (isShared(name)) {
            return kawa.loadClass(name);
        }
        return super.loadClass(name, resolve);
    }

    private static boolean isShared(String className) {
        return SHARED_PACKAGES.stream().anyMatch(className::startsWith);
    }
}
