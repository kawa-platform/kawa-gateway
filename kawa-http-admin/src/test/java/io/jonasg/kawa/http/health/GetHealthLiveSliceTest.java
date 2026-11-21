package io.jonasg.kawa.http.health;

import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.Optional;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

class GetHealthLiveSliceTest extends HealthSliceTestBase {

    @Test
    void answersUpWhileTheClusterIsUnreachable() throws Exception {
        // given
        probe.listenerBound = false;
        probe.configLoaded = false;
        probe.timeSinceClusterRefresh = Optional.empty();
        startServer();

        // when
        HttpResponse<String> response = get("/health/live");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("{\"status\":\"UP\"}");
    }

    @Test
    void answersWithoutCredentials() throws Exception {
        // given
        startServer();

        // when
        HttpResponse<String> response = get("/health/live");

        // then
        assertThat(response).hasStatusCode(200);
    }
}
