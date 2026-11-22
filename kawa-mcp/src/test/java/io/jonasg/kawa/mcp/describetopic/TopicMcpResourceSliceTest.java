package io.jonasg.kawa.mcp.describetopic;

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

class TopicMcpResourceSliceTest extends McpSliceTestBase {

    @Test
    void readsABrokerTopicByItsName() throws Exception {
        // given
        brokerAdmin.states.put("orders-v2", new TopicState(3, 2, Map.of("retention.ms", "60000")));
        startServer();

        // when
        var readResp = readResource("kafka://topics/orders-v2");

        // then
        assertThat(readResp).withFailMessage(() -> "resources/read of orders-v2 was not answered with 200").hasStatusCode(200);
        assertThatJson(readResp.body()).inPath("result.contents[0].text").isString()
                .satisfies(text -> assertThatJson(text)
                        .withFailMessage(() -> "orders-v2 was not described as the physical topic: " + readResp.body())
                        .isEqualTo(json("""
                                {
                                  "topicName": "orders-v2",
                                  "physicalTopic": {
                                    "name": "orders-v2",
                                    "partitions": 3,
                                    "replicationFactor": 2,
                                    "settings": {"retention.ms": "60000"}
                                  },
                                  "shadowsPhysicalTopic": false
                                }""")));
    }

    @Test
    void readsATopicWhoseNameIsPercentEncoded() throws Exception {
        // given
        brokerAdmin.states.put("orders-v2", new TopicState(3, 2, Map.of()));
        startServer();

        // when
        var readResp = readResource("kafka://topics/orders%2Dv2");

        // then
        assertThatJson(readResp.body()).inPath("result.contents[0].text").isString()
                .satisfies(text -> assertThatJson(text).inPath("topicName")
                        .withFailMessage(() -> "orders%2Dv2 was not read as orders-v2: " + readResp.body())
                        .isEqualTo("orders-v2"));
    }

    @Test
    void answersResourceNotFoundForAVirtualTopicName() throws Exception {
        // given
        brokerAdmin.states.put("orders-v2", new TopicState(3, 2, Map.of()));
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2", new HeaderEqualsFilterConfig("region", "eu"), false,
                        new JsonFormatConfig())));
        startServer();

        // when
        var readResp = readResource("kafka://topics/orders");

        // then
        assertThatJson(readResp.body()).inPath("error.code")
                .withFailMessage(() -> "a virtual topic name was not refused as a resource that does not exist: " + readResp.body())
                .isEqualTo(-32002);
    }

    @Test
    void answersResourceNotFoundForATopicTheBrokerDoesNotKnow() throws Exception {
        // given
        startServer();

        // when
        var readResp = readResource("kafka://topics/missing");

        // then
        assertThatJson(readResp.body()).inPath("error.code")
                .withFailMessage(() -> "an unknown topic was not refused as a resource that does not exist: " + readResp.body())
                .isEqualTo(-32002);
    }
}
