package io.jonasg.kawa.mcp.listtopics;

import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.governance.TopicDescriber.TopicState;
import io.jonasg.kawa.mcp.McpSliceTestBase;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.json;

class TopicsMcpResourceSliceTest extends McpSliceTestBase {

    @Test
    void answersTheBrokerTopicsWithoutTheVirtualTopics() throws Exception {
        // given
        brokerAdmin.states.put("orders-v2", new TopicState(3, 2, Map.of()));
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2", new HeaderEqualsFilterConfig("region", "eu"), false,
                        new JsonFormatConfig())));
        cache = cacheWith(topic("orders-v2", 4, 2));
        startServer();

        // when
        var readResp = readResource("kawa://topics");

        // then
        assertThat(readResp).withFailMessage(() -> "resources/read of kawa://topics was not answered with 200").hasStatusCode(200);
        assertThatJson(readResp.body()).inPath("result.contents[0].text").isString()
                .satisfies(text -> assertThatJson(text)
                        .withFailMessage(() -> "kawa://topics did not list only the broker topics: " + readResp.body())
                        .isEqualTo(json("""
                                {
                                  "virtualTopics": [],
                                  "physicalTopics": [
                                    {"name": "orders-v2", "partitions": 4, "replicationFactor": 2}
                                  ]
                                }""")));
    }
}
