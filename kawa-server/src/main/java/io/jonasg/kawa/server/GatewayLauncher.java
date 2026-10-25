package io.jonasg.kawa.server;

import io.jonasg.kawa.config.BootstrapConfig;
import io.jonasg.kawa.config.ConfigLoader;
import io.jonasg.kawa.server.plugin.PluginDirectory;
import io.jonasg.kawa.server.plugin.PluginLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

/// Entry point. The `--config` file is the static [BootstrapConfig]: cluster bootstrap servers,
/// broker credentials, config topic name, listeners, advertised endpoint and admin.
/// Virtual topics, RBAC and client auth are `DynamicConfig`, read from the config topic by [KafkaGateway]
/// and update live; an empty config topic on first boot is valid. External plugins are loaded from
/// `--plugins <dir>`, `KAWA_PLUGIN_DIR` or `/opt/kawa/plugins` (see [PluginDirectory]).
public final class GatewayLauncher {

    private static final Logger log = LoggerFactory.getLogger(GatewayLauncher.class);

    private GatewayLauncher() {
    }

    static void main(String[] args) throws Exception {
        Path configPath = parseConfigPath(args);
        BootstrapConfig bootstrap = new ConfigLoader().load(configPath);
        var pluginDirectory = PluginDirectory.resolve(args, System.getenv()).orElse(null);
        var plugins = new PluginLoader(GatewayLauncher.class.getClassLoader(), pluginDirectory).loadAll();
        var gateway = new KafkaGateway(bootstrap, plugins.authenticationProviders());

        var shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down kawa gateway");
            gateway.stop();
            shutdown.countDown();
        }));

        gateway.start();
        log.info("kawa gateway {} started (bootstrap {})", version(), configPath);
        shutdown.await();
    }

    /// Version from the shaded jar manifest; `dev` when running from an IDE or unpacked classes.
    static String version() {
        return Objects.requireNonNullElse(GatewayLauncher.class.getPackage().getImplementationVersion(), "dev");
    }

    private static Path parseConfigPath(String[] args) {
        String path = "config.yaml";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--config") && i + 1 < args.length) {
                path = args[i + 1];
            }
        }
        return Path.of(path);
    }
}
