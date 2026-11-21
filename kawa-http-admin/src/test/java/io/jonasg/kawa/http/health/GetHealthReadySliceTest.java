package io.jonasg.kawa.http.health;

import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

class GetHealthReadySliceTest extends HealthSliceTestBase {

    @Test
    void answersUpWhenEveryConditionHolds() throws Exception {
        // given
        probe.timeSinceClusterRefresh = Optional.of(Duration.ofSeconds(4));
        startServer();

        // when
        HttpResponse<String> response = get("/health/ready");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("{\"status\":\"UP\",\"failures\":[]}");
    }

    @Test
    void answers503NamingTheListenerWhenItIsNotBound() throws Exception {
        // given
        probe.listenerBound = false;
        startServer();

        // when
        HttpResponse<String> response = get("/health/ready");

        // then
        assertThat(response).hasStatusCode(503);
        assertThatJson(response.body()).node("failures[0].condition").isEqualTo("listener");
    }

    @Test
    void answers503NamingTheConfigWhenItIsNotLoaded() throws Exception {
        // given
        probe.configLoaded = false;
        startServer();

        // when
        HttpResponse<String> response = get("/health/ready");

        // then
        assertThat(response).hasStatusCode(503);
        assertThatJson(response.body()).node("status").isEqualTo("DOWN");
        assertThatJson(response.body()).node("failures[0].condition").isEqualTo("config");
    }

    @Test
    void answers503NamingTheClusterViewWhenItWasNeverRefreshed() throws Exception {
        // given
        probe.timeSinceClusterRefresh = Optional.empty();
        startServer();

        // when
        HttpResponse<String> response = get("/health/ready");

        // then
        assertThat(response).hasStatusCode(503);
        assertThatJson(response.body()).node("failures[0].condition").isEqualTo("clusterView");
    }

    @Test
    void answers503WithTheAgeOfAStaleClusterView() throws Exception {
        // given
        probe.timeSinceClusterRefresh = Optional.of(Duration.ofSeconds(45));
        startServer();

        // when
        HttpResponse<String> response = get("/health/ready");

        // then
        assertThat(response).hasStatusCode(503);
        assertThatJson(response.body()).node("failures[0].condition").isEqualTo("clusterView");
        assertThatJson(response.body()).node("failures[0].detail").isEqualTo(
                "the cluster view is stale: last successful refresh 45 s ago");
    }

    @Test
    void answers200OnceAStaleClusterViewIsRefreshedAgain() throws Exception {
        // given
        probe.timeSinceClusterRefresh = Optional.of(Duration.ofSeconds(45));
        startServer();
        assertThat(get("/health/ready")).hasStatusCode(503);

        // when
        probe.timeSinceClusterRefresh = Optional.of(Duration.ofSeconds(2));
        HttpResponse<String> response = get("/health/ready");

        // then
        assertThat(response).hasStatusCode(200);
    }
}
