package io.jonasg.kawa.http.health;

import io.jonasg.kawa.http.web.NettyHttpServer;
import io.jonasg.kawa.http.web.Router;

import java.util.Optional;

/// Serves only the health probes, for when the admin surface is off or not yet started. The probe
/// routes are public, so the authentication provider is never consulted and admits nobody.
public final class HealthServer implements AutoCloseable {

    private final NettyHttpServer http;

    public HealthServer(String host, int port, HealthProbe probe) {
        var router = new Router();
        HealthRoutes.register(router, probe);
        this.http = new NettyHttpServer(host, port, router, null, request -> Optional.empty());
    }

    public void start() throws InterruptedException {
        http.start();
    }

    public int boundPort() {
        return http.boundPort();
    }

    public void stop() {
        http.stop();
    }

    @Override
    public void close() {
        stop();
    }
}
