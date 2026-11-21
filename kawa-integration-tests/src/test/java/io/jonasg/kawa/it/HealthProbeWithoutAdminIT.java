package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AdminConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/// The health probes of a gateway with the admin HTTP server disabled: the probes are still served,
/// on the admin address.
class HealthProbeWithoutAdminIT extends GatewayTestSupport {

    private static final int HEALTH_PORT = freePort();

    private final HttpClient http = HttpClient.newHttpClient();

    @Override
    protected AdminConfig adminConfig() {
        return new AdminConfig(false, "127.0.0.1", HEALTH_PORT, null);
    }

    @Test
    void servesReadyOnTheAdminAddressWhenAdminIsDisabled() throws Exception {
        // given - a gateway with the admin HTTP server disabled

        // when / then - the readiness probe answers once the gateway has loaded its config and cluster view
        eventually(() -> {
            HttpResponse<String> ready = http.send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + HEALTH_PORT + "/health/ready")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());

            assertThat(ready.statusCode()).isEqualTo(200);
        });
    }

    private static int freePort() {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("no free port for the health probes", e);
        }
    }
}
