package io.jonasg.kawa.server.plugin;

import io.jonasg.kawa.plugin.KawaPlugin;
import io.jonasg.kawa.plugin.KawaPluginContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/// Discovers every [KawaPlugin] on the classpath with [ServiceLoader] and registers it. Fails fast: a plugin
/// that cannot be instantiated or whose [KawaPlugin#register] throws aborts startup instead of letting the
/// gateway run without it.
public final class PluginLoader {

    private static final Logger log = LoggerFactory.getLogger(PluginLoader.class);

    private final ClassLoader classLoader;

    public PluginLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public List<KawaPlugin> loadAll() {
        var context = new GatewayPluginContext();
        List<KawaPlugin> plugins = new ArrayList<>();
        for (KawaPlugin plugin : ServiceLoader.load(KawaPlugin.class, classLoader)) {
            String name = plugin.getClass().getName();
            try {
                plugin.register(context);
            } catch (RuntimeException e) {
                throw new IllegalStateException("Plugin " + name + " failed to register", e);
            }
            log.info("Loaded plugin {}", name);
            plugins.add(plugin);
        }
        log.info("{} plugin(s) loaded", plugins.size());
        return List.copyOf(plugins);
    }

    private static final class GatewayPluginContext implements KawaPluginContext {
    }
}
