package io.jonasg.kawa.http.health;

import org.junit.jupiter.api.AfterEach;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;

/// Boots a real [HealthServer] on an ephemeral port over a [FakeHealthProbe] and sends real HTTP
/// requests to it.
abstract class HealthSliceTestBase {

    protected final FakeHealthProbe probe = new FakeHealthProbe();
    private final HttpClient client = HttpClient.newHttpClient();
    private HealthServer server;

    protected void startServer() throws InterruptedException {
        server = new HealthServer("127.0.0.1", 0, probe);
        server.start();
    }

    protected HttpResponse<String> get(String path) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + path)).GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop();
        }
    }

    static final class FakeHealthProbe implements HealthProbe {

        boolean listenerBound = true;
        boolean configLoaded = true;
        Optional<Duration> timeSinceClusterRefresh = Optional.of(Duration.ZERO);

        @Override
        public boolean listenerBound() {
            return listenerBound;
        }

        @Override
        public boolean configLoaded() {
            return configLoaded;
        }

        @Override
        public Optional<Duration> timeSinceClusterRefresh() {
            return timeSinceClusterRefresh;
        }
    }
}
