package io.jonasg.kawa.server;

import io.jonasg.kawa.config.ConfigLoader;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.core.Gateway;
import io.jonasg.kawa.server.plugin.PluginLoader;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/// Entry point. The `--config` file is the static bootstrap: cluster bootstrap servers,
/// broker credentials, config topic name, listeners, advertised endpoint and admin.
/// Virtual topics, RBAC and client auth are read from the config topic by [KafkaGateway]
/// and update live; an empty config topic on first boot is valid.
public final class GatewayLauncher {

    private GatewayLauncher() {
    }

    static void main(String[] args) throws Exception {
        Path configPath = parseConfigPath(args);
        GatewayConfig bootstrap = new ConfigLoader().load(configPath);
        new PluginLoader(GatewayLauncher.class.getClassLoader()).loadAll();
        Gateway gateway = new KafkaGateway(bootstrap);

        var shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            gateway.stop();
            shutdown.countDown();
        }));

        gateway.start();
        System.out.println("kawa gateway started (bootstrap " + configPath + ", dynamic config from topic)");
        shutdown.await();
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
