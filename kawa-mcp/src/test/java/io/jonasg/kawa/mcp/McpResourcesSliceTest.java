package io.jonasg.kawa.mcp;

import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

class McpResourcesSliceTest extends McpSliceTestBase {

    @Test
    void completesTopicNamesFromThePhysicalTopicsStartingWithTheTypedPrefix() throws Exception {
        // given
        cache = cacheWith(topic("orders-v2", 1, 1), topic("orders-archive", 1, 1), topic("payments", 1, 1));
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2", new HeaderEqualsFilterConfig("region", "eu"), false,
                        new JsonFormatConfig())));
        startServer();

        // when
        var completeResp = rpc("completion/complete", """
                {"ref":{"type":"ref/resource","uri":"kafka://topics/{topicName}"},
                 "argument":{"name":"topicName","value":"orders"}}""");

        // then
        assertThat(completeResp).withFailMessage(() -> "completion/complete was not answered with 200").hasStatusCode(200);
        assertThatJson(completeResp.body()).inPath("result.completion.values")
                .withFailMessage(() -> "completion/complete did not offer the physical topics starting with 'orders': " + completeResp.body())
                .isEqualTo(List.of("orders-archive", "orders-v2"));
    }

    @Test
    void listsTheTopicAndConsumerGroupResourcesAndTheirTemplates() throws Exception {
        // given
        startServer();

        // when
        var listResp = rpc("resources/list", "{}");
        var templatesResp = rpc("resources/templates/list", "{}");

        // then
        assertThat(listResp).withFailMessage(() -> "resources/list was not answered with 200").hasStatusCode(200);
        assertThatJson(listResp.body()).inPath("result.resources[*].uri")
                .isArray()
                .withFailMessage(() -> "resources/list does not advertise the topics and consumer groups: " + listResp.body())
                .containsExactlyInAnyOrder("kafka://topics", "kafka://consumer-groups");
        assertThatJson(templatesResp.body()).inPath("result.resourceTemplates[*].uriTemplate")
                .isArray()
                .withFailMessage(() -> "resources/templates/list does not advertise the single-item templates: " + templatesResp.body())
                .containsExactlyInAnyOrder("kafka://topics/{topicName}", "kafka://consumer-groups/{groupId}");
    }
}
