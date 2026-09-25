package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AdminConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static net.javacrumbs.jsonunit.core.Option.IGNORING_ARRAY_ORDER;
import static org.assertj.core.api.Assertions.assertThat;

class AdminGetTopicsIT extends AdminHTTPTestSupport {

    @Override
    protected AdminConfig adminConfig() {
        return new AdminConfig(true, "127.0.0.1", 0, null);
    }

    @Override
    protected Map<String, String> virtualTopics() {
        return Map.of("orders", "orders-v2");
    }

    @Override
    protected List<NewTopic> initialTopics() {
        return List.of(new NewTopic("orders-v2", 1, (short) 1));
    }

    @Test
    void exposesVirtualTopicsOverHttp() {
        // given
        var request = reqBuilder("/topics").GET().build();

        // when — the gateway's metadata cache picks up the pre-created topic on its next refresh
        Awaitility.await().logging().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            HttpResponse<String> response = httpExec(request);

            // then
            assertThat(response.statusCode()).isEqualTo(200);
            assertThatJson(withoutInternalTopics(response.body())).when(IGNORING_ARRAY_ORDER).isEqualTo("""
                    [
                        {
                          "type" : "virtual",
                          "name" : "orders",
                          "partitions" : 1,
                          "replicationFactor" : 1,
                          "filter" : null,
                          "physicalTopic" : "orders-v2",
                          "exposePhysicalTopic" : false,
                          "valueFormat" : null
                        },
                        {
                          "type" : "physical",
                          "name" : "orders-v2",
                          "partitions" : 1,
                          "replicationFactor" : 1,
                          "filter" : null,
                          "physicalTopic" : null,
                          "exposePhysicalTopic" : null,
                          "valueFormat" : null
                        }
                    ]
                    """);
        });
    }

    /// Drops `__`-prefixed (internal) topics from the admin `/topics` payload so the
    /// assertion only sees user-visible topics.
    private static String withoutInternalTopics(String json) {
        var mapper = JsonMapper.builder().build();
        ArrayNode visible = mapper.createArrayNode();
        for (JsonNode topic : mapper.readTree(json)) {
            if (!topic.get("name").asString().startsWith("__")) {
                visible.add(topic);
            }
        }
        return mapper.writeValueAsString(visible);
    }
}
