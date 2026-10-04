package io.jonasg.kawa.server.plugin;

import io.jonasg.kawa.http.auth.AuthenticationProviderRegistry;
import io.jonasg.kawa.plugin.KawaPlugin;
import io.jonasg.kawa.plugin.KawaPluginContext;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Stream;

/// Discovers and registers every [KawaPlugin]:
///
/// - **built-in plugins** on kawa's own classpath (e.g. the `none`/`oauth` authentication providers)
/// - **external plugins** in the plugin directory: every subdirectory is one plugin made of all the JARs
///   in it, and every top-level JAR is a single-JAR plugin. Each gets its own [PluginClassLoader], so it
///   sees only the JDK, the plugin API, slf4j and its own JARs.
///
/// Fails fast: a plugin that cannot be loaded (missing class, broken service entry) or whose
/// [KawaPlugin#register] throws aborts startup instead of letting the gateway run without it.
public final class PluginLoader {

    private static final Logger log = LoggerFactory.getLogger(PluginLoader.class);
    private static final String SERVICE_FILE = "META-INF/services/" + KawaPlugin.class.getName();

    private final ClassLoader classLoader;
    private final @Nullable Path pluginDirectory;

    /// A loader for the built-in plugins on `classLoader` only.
    public PluginLoader(ClassLoader classLoader) {
        this(classLoader, null);
    }

    public PluginLoader(ClassLoader classLoader, @Nullable Path pluginDirectory) {
        this.classLoader = classLoader;
        this.pluginDirectory = pluginDirectory;
    }

    public LoadedPlugins loadAll() {
        var authenticationProviders = new AuthenticationProviderRegistry();
        List<KawaPlugin> plugins = new ArrayList<>();
        for (var kawaPlugin : ServiceLoader.load(KawaPlugin.class, classLoader)) {
            register(kawaPlugin, new GatewayPluginContext(authenticationProviders, null), "classpath");
            plugins.add(kawaPlugin);
        }
        for (Path location : externalPlugins()) {
            plugins.addAll(loadExternal(location, authenticationProviders));
        }
        log.info("{} plugin(s) loaded", plugins.size());
        return new LoadedPlugins(List.copyOf(plugins), authenticationProviders);
    }

    private List<KawaPlugin> loadExternal(Path location, AuthenticationProviderRegistry authenticationProviders) {
        var loader = new PluginClassLoader(location, jars(location), classLoader);
        var context = new GatewayPluginContext(authenticationProviders, loader);
        List<KawaPlugin> plugins = new ArrayList<>();
        try {
            for (KawaPlugin plugin : ServiceLoader.load(KawaPlugin.class, loader)) {
                IsolatedPluginCalls.withContextClassLoader(loader, () -> {
                    register(plugin, context, location.toString());
                    return null;
                });
                plugins.add(plugin);
            }
        } catch (ServiceConfigurationError | LinkageError e) {
            throw new IllegalStateException("plugin " + location + " failed to load: " + e, e);
        }
        if (plugins.isEmpty()) {
            throw new IllegalStateException("plugin " + location + " contains no " + SERVICE_FILE + " entry");
        }
        return plugins;
    }

    private static void register(KawaPlugin plugin, KawaPluginContext context, String source) {
        String name = plugin.getClass().getName();
        try {
            plugin.register(context);
        } catch (RuntimeException | LinkageError e) {
            throw new IllegalStateException("plugin " + name + " from " + source + " failed to register: " + e, e);
        }
        log.info("Loaded plugin {} from {}", name, source);
    }

    /// The plugin directory's entries that are plugins, in name order.
    private List<Path> externalPlugins() {
        if (pluginDirectory == null) {
            return List.of();
        }
        try (Stream<Path> entries = Files.list(pluginDirectory)) {
            return entries.filter(entry -> Files.isDirectory(entry) || isJar(entry)).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException("could not list plugin directory " + pluginDirectory, e);
        }
    }

    private static List<URL> jars(Path location) {
        if (!Files.isDirectory(location)) {
            return List.of(url(location));
        }
        try (Stream<Path> files = Files.list(location)) {
            return files.filter(PluginLoader::isJar).sorted().map(PluginLoader::url).toList();
        } catch (IOException e) {
            throw new UncheckedIOException("could not list plugin " + location, e);
        }
    }

    private static boolean isJar(Path path) {
        return Files.isRegularFile(path) && path.getFileName().toString().endsWith(".jar");
    }

    private static URL url(Path path) {
        try {
            return path.toUri().toURL();
        } catch (MalformedURLException e) {
            throw new IllegalStateException("invalid plugin path " + path, e);
        }
    }

    /// What a plugin registers into. Contributions of an external plugin are wrapped so kawa calls them
    /// with the plugin's classloader as the thread context classloader.
    private record GatewayPluginContext(
            AuthenticationProviderRegistry authenticationProviders,
            @Nullable ClassLoader isolatedLoader) implements KawaPluginContext {

        @Override
        public void registerAuthenticationProvider(AuthenticationProviderFactory factory) {
            authenticationProviders.register(isolatedLoader == null
                    ? factory
                    : IsolatedPluginCalls.isolate(factory, isolatedLoader));
            log.info("Registered admin authentication provider '{}' ({})", factory.name(), factory.getClass().getName());
        }
    }
}
