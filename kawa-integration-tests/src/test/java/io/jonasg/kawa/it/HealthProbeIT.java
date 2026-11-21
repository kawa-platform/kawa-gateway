package io.jonasg.kawa.it;

import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/// The health probes of a gateway with the admin HTTP server enabled, served on the admin port.
class HealthProbeIT extends AdminHTTPTestSupport {

    @Test
    void answersLiveAndReadyOnTheAdminPort() throws Exception {
        // given - a gateway that has loaded its config and refreshed its cluster view

        // when - the readiness probe is asked
        eventually(() -> {
            HttpResponse<String> ready = httpExec(reqBuilder("/health/ready").GET().build());

            // then
            assertThat(ready.statusCode()).isEqualTo(200);
        });

        // and - the liveness probe is asked
        HttpResponse<String> live = httpExec(reqBuilder("/health/live").GET().build());

        // then
        assertThat(live.statusCode()).isEqualTo(200);
    }
}
