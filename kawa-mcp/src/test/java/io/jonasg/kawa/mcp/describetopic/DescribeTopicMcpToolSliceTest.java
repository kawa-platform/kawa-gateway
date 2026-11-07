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

class DescribeTopicMcpToolSliceTest extends McpSliceTestBase {

    @Test
    void advertisesDescribeTopicWithItsOutputSchema() throws Exception {
        // given
        ordersV2ExistsOnTheBroker();
        startServer();

        // when
        var toolsResp = rpc("tools/list", "{}");

        // then
        assertThat(toolsResp).withFailMessage(() -> "tools/list was not answered with 200").hasStatusCode(200);
        assertThatJson(toolsResp.body()).inPath("result.tools[*].name").isArray()
                .withFailMessage(() -> "describe_topic is not advertised by tools/list").contains("describe_topic");
        assertThatJson(toolsResp.body()).inPath("result.tools[?(@.name=='describe_topic')].outputSchema")
                .withFailMessage(() -> "describe_topic does not publish the TopicDescriptionView output schema")
                .isEqualTo(json(
                """
                         [{
                          "additionalProperties" : false,
                          "type" : "object",
                          "properties" : {
                            "topicName" : {
                              "type" : "string"
                            },
                            "virtualTopic" : {
                              "type" : "object",
                              "properties" : {
                                "topicName" : {
                                  "type" : "string"
                                },
                                "filter" : {
                                  "type" : "object",
                                  "properties" : {
                                    "type" : {
                                      "type" : "string"
                                    },
                                    "description" : {
                                      "type" : "string"
                                    }
                                  },
                                  "required" : [ "type", "description" ],
                                  "additionalProperties" : false
                                },
                                "valueFormat" : {
                                  "type" : "string",
                                  "enum" : [ "json", "avro", "raw" ]
                                },
                                "exposesPhysicalTopic" : {
                                  "type" : "boolean"
                                }
                              },
                              "required" : [ "topicName", "valueFormat", "exposesPhysicalTopic" ],
                              "additionalProperties" : false
                            },
                            "physicalTopic" : {
                              "type" : "object",
                              "properties" : {
                                "name" : {
                                  "type" : "string"
                                },
                                "partitions" : {
                                  "type" : "integer"
                                },
                                "replicationFactor" : {
                                  "type" : "integer"
                                },
                                "settings" : {
                                  "type" : "object",
                                  "additionalProperties" : {
                                    "type" : "string"
                                  }
                                }
                              },
                              "required" : [ "name", "partitions", "replicationFactor", "settings" ],
                              "additionalProperties" : false
                            },
                            "shadowsPhysicalTopic" : {
                              "type" : "boolean"
                            }
                          },
                          "required" : [ "topicName", "physicalTopic", "shadowsPhysicalTopic" ]
                        }]"""));
    }

    @Test
    void describesPhysicalTopic() throws Exception {
        // given
        ordersV2ExistsOnTheBroker();
        virtualTopics = ordersVirtualTopic();
        cache = cacheWith(topic("orders-v2", 4, 2));
        startServer();

        // when
        var describeResp = callToolWithArgs("describe_topic", "{\"topicName\": \"orders-v2\"}");

        // then
        assertThat(describeResp).withFailMessage(() -> "describe_topic of 'orders-v2' was not answered with 200")
                .hasStatusCode(200);
        assertThatJson(describeResp.body()).inPath("result.isError")
                .withFailMessage(() -> "describe_topic of 'orders-v2' was a tool error").isEqualTo(false);
        assertThatJson(describeResp.body()).inPath("result.structuredContent")
                .withFailMessage(() -> "describe_topic of 'orders-v2' did not return the physical topic")
                .isEqualTo("""
                {
                   "topicName": "orders-v2",
                   "physicalTopic": {
                     "name": "orders-v2",
                     "partitions": 3,
                     "replicationFactor": 2,
                     "settings": {
                       "retention.ms": "60000"
                     }
                   },
                   "shadowsPhysicalTopic": false
                 }""");
        assertThatJson(describeResp.body()).inPath("result.content[0].text").isString()
                .satisfies(text -> assertThatJson(text)
                        .withFailMessage(() -> "the text content is not the structured content as JSON: " + text)
                        .isEqualTo(json(structuredContent(describeResp.body()))));
    }

    @Test
    void describesATopicThatDoesNotExist() throws Exception {
        // given
        ordersV2ExistsOnTheBroker();
        virtualTopics = ordersVirtualTopic();
        cache = cacheWith(topic("orders-v2", 4, 2));
        startServer();

        // when
        var describeResp = callToolWithArgs("describe_topic", "{\"topicName\": \"does-not-exist\"}");

        // then
        assertThat(describeResp).withFailMessage(() -> "describe_topic of a missing topic was not answered with 200")
                .hasStatusCode(200);
        assertThatJson(describeResp.body()).inPath("result.isError")
                .withFailMessage(() -> "describe_topic of a missing topic was not a tool error").isEqualTo(true);
        assertThatJson(describeResp.body()).inPath("result.structuredContent")
                .withFailMessage(() -> "describe_topic of a missing topic returned structured content").isAbsent();
        assertThatJson(describeResp.body()).inPath("result.content[0].text").isString()
                .satisfies(text -> assertThat(text)
                        .withFailMessage(() -> "describe_topic of a missing topic did not explain the failure: " + text)
                        .isEqualTo("Unable to describe an non existing topic: does-not-exist"));
    }

    @Test
    void describesVirtualTopic() throws Exception {
        // given
        ordersV2ExistsOnTheBroker();
        virtualTopics = ordersVirtualTopic();
        cache = cacheWith(topic("orders-v2", 4, 2));
        startServer();

        // when
        var describeResp = callToolWithArgs("describe_topic", "{\"topicName\": \"orders\"}");

        // then
        assertThat(describeResp).withFailMessage(() -> "describe_topic of 'orders' was not answered with 200")
                .hasStatusCode(200);
        assertThatJson(describeResp.body()).inPath("result.isError")
                .withFailMessage(() -> "describe_topic of 'orders' was a tool error").isEqualTo(false);
        assertThatJson(describeResp.body()).inPath("result.structuredContent")
                .withFailMessage(() -> "describe_topic of 'orders' did not return the virtual topic's physical target")
                .isEqualTo("""
                {
                   "topicName": "orders",
                   "virtualTopic": {
                     "topicName": "orders",
                     "filter": {
                       "type": "headerEquals",
                       "description": "header 'region' equals 'eu'"
                     },
                     "valueFormat": "json",
                     "exposesPhysicalTopic": true
                   },
                   "physicalTopic": {
                     "name": "orders-v2",
                     "partitions": 3,
                     "replicationFactor": 2,
                     "settings": {
                       "retention.ms": "60000"
                     }
                   },
                   "shadowsPhysicalTopic": false
                }""");
        assertThatJson(describeResp.body()).inPath("result.content[0].text").isString()
                .satisfies(text -> assertThatJson(text)
                        .withFailMessage(() -> "the text content is not the structured content as JSON: " + text)
                        .isEqualTo(json(structuredContent(describeResp.body()))));
    }

    @Test
    void describesAVirtualTopicThatShadowsAPhysicalTopic() throws Exception {
        // given
        ordersV2ExistsOnTheBroker();
        topicAdmin.states.put("orders", new TopicState(1, 1, Map.of()));
        virtualTopics = ordersVirtualTopic();
        cache = cacheWith(topic("orders-v2", 4, 2));
        startServer();

        // when
        var describeResp = callToolWithArgs("describe_topic", "{\"topicName\": \"orders\"}");

        // then
        assertThat(describeResp).withFailMessage(() -> "describe_topic of 'orders' was not answered with 200")
                .hasStatusCode(200);
        assertThatJson(describeResp.body()).inPath("result.structuredContent.shadowsPhysicalTopic")
                .withFailMessage(() -> "describe_topic of 'orders' did not report the physical topic it hides")
                .isEqualTo(true);
    }

    @Test
    void describesAVirtualTopicWhosePhysicalTopicIsMissingOnTheBroker() throws Exception {
        // given
        virtualTopics = ordersVirtualTopic();
        cache = cacheWith(topic("orders-v2", 4, 2));
        startServer();

        // when
        var describeResp = callToolWithArgs("describe_topic", "{\"topicName\": \"orders\"}");

        // then
        assertThat(describeResp).withFailMessage(() -> "describe_topic of 'orders' was not answered with 200")
                .hasStatusCode(200);
        assertThatJson(describeResp.body()).inPath("result.isError")
                .withFailMessage(() -> "describe_topic of 'orders' was a tool error").isEqualTo(false);
        assertThatJson(describeResp.body()).inPath("result.structuredContent.physicalTopic")
                .withFailMessage(() -> "describe_topic of 'orders' did not report the missing physical topic as unknown")
                .isEqualTo("""
                {
                  "name": "orders-v2",
                  "partitions": 0,
                  "replicationFactor": 0,
                  "settings": {}
                }""");
    }

    /// The broker holds `orders-v2` with 3 partitions, replication factor 2 and `retention.ms` 60000.
    private void ordersV2ExistsOnTheBroker() {
        topicAdmin.states.put("orders-v2", new TopicState(3, 2, Map.of("retention.ms", "60000")));
    }

    /// The virtual topic `orders`, which maps to `orders-v2` and keeps only records with `region` = `eu`.
    private static VirtualTopicManager ordersVirtualTopic() {
        return new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2", new HeaderEqualsFilterConfig("region", "eu"), true,
                        new JsonFormatConfig())));
    }
}
