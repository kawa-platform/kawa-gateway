package io.jonasg.kawa.it;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class TopicsAdminApiIT extends AdminHTTPTestSupport {

    @Test
    void createsAndDeletesPhysicalTopicViaAdminApi() throws Exception {
        // given - a physical topic
        var createResp = httpExec(
                reqBuilder("/topics")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                """
                                        {
                                          "type": "physical",
                                          "name": "orders",
                                          "partitions": 3,
                                          "replicationFactor": 1
                                        }
                                        """))
                        .build()
        );

        // and - the topic is listed as soon as the creating call returned. This read is deliberately
        // immediate, not awaited: the creation itself waits until the gateway's metadata has the topic.
        assertThat(createResp.statusCode()).isEqualTo(201);
        HttpResponse<String> list = httpExec(reqBuilder("/topics").GET().build());
        assertThat(list.body())
                .withFailMessage(() -> "Topic 'orders' is not listed straight after it was created: " + list.body())
                .contains("\"type\":\"physical\"", "\"name\":\"orders\"");

        // and - the topic exists on the broker
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() ->
                        assertThat(brokerAdmin.listTopics().names().get()).contains("orders"));

        // when - the topic is deleted through the unified /topics surface
        var deleteResp = httpExec(
                reqBuilder("/topics/orders")
                        .DELETE()
                        .build()
        );

        // then - the topic is gone from the broker
        assertThat(deleteResp.statusCode()).isEqualTo(204);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() ->
                        assertThat(brokerAdmin.listTopics().names().get()).doesNotContain("orders"));
    }

    @Test
    void createsUpdatesAndDeletesVirtualTopicViaAdminApi() throws Exception {
        // given - a fresh gateway with the admin HTTP server enabled

        // when - a virtual topic is created through the unified /topics surface
        var createReq = httpExec(
                reqBuilder("/topics?consistency=applied")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                """
                                        {
                                          "type": "virtual",
                                          "name": "orders",
                                          "topic": "orders-v2"
                                        }
                                        """))
                        .build());

        // then - the virtual topic is listed by GET /topics once the consumer applies it
        assertThat(createReq.statusCode()).isEqualTo(201);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    HttpResponse<String> list = httpExec(reqBuilder("/topics").GET().build());
                    assertThat(list.body()).contains("\"type\":\"virtual\"", "\"name\":\"orders\"");
                });

        // when - the virtual topic config is updated
        var updateResp = httpExec(
                reqBuilder("/topics/orders?consistency=applied")
                        .PUT(HttpRequest.BodyPublishers.ofString("""
                                {
                                  "type": "virtual",
                                  "topic": "orders-v3"
                                }
                                """))
                        .build());

        // then - the update is persisted
        assertThat(updateResp.statusCode()).isEqualTo(200);

        // when - the virtual topic is deleted
        var deleteResp = httpExec(
                reqBuilder("/topics/orders?consistency=applied")
                        .DELETE()
                        .build());

        // then - the virtual topic is no longer listed
        assertThat(deleteResp.statusCode()).isEqualTo(204);
        HttpResponse<String> list = httpExec(reqBuilder("/topics").GET().build());
        assertThat(list.body()).doesNotContain("\"name\":\"orders\"");
    }

    @Test
    void patchesAndRenamesVirtualTopicViaAdminApi() throws Exception {
        // given - a virtual topic persisted through the admin API
        var createResp = httpExec(
                reqBuilder("/topics")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                """
                                        {
                                          "type": "virtual",
                                          "name": "audit",
                                          "topic": "audit-v1"
                                        }
                                        """))
                        .build());
        assertThat(createResp.statusCode()).isEqualTo(201);

        // when - the virtual topic is updated and renamed in one request
        var updateResp = httpExec(
                reqBuilder("/topics/audit?consistency=applied")
                        .method("PATCH", HttpRequest.BodyPublishers.ofString(
                                """
                                        {
                                          "name": "eu-audit",
                                          "topic": "audit-v2",
                                          "filter": {
                                            "type": "headerEquals",
                                            "header": "region",
                                            "value": "eu"
                                          },
                                          "exposePhysicalTopic": true
                                        }
                                        """))
                        .build());

        // then - the resulting configuration is returned and applied to the gateway
        assertThat(updateResp.statusCode()).isEqualTo(200);
        assertThat(updateResp.body()).contains(
                "\"topic\":\"audit-v2\"",
                "\"exposePhysicalTopic\":true");
        HttpResponse<String> list = httpExec(reqBuilder("/topics").GET().build());
        assertThat(list.body()).contains("\"type\":\"virtual\"", "\"name\":\"eu-audit\"");
        assertThat(list.body()).doesNotContain("\"name\":\"audit\"");
    }
}
