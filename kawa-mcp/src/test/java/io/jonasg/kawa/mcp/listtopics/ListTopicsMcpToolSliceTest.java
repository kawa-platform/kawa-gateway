package io.jonasg.kawa.mcp.listtopics;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.mcp.McpSliceTestBase;
import io.jonasg.kawa.mcp.McpTool;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.net.http.HttpResponse;
import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.json;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/// Slice tests for [ListTopicsMcpTool]: real JSON-RPC requests to `POST /mcp` on a booted admin server.
class ListTopicsMcpToolSliceTest extends McpSliceTestBase {

    @Test
    void advertisesListTopicsWithItsOutputSchema() throws Exception {
        // given
        startServer();

        // when
        var toolsResp = rpc("tools/list", "{}");

        // then
        assertThat(toolsResp).hasStatusCode(200);
        assertThatJson(toolsResp.body()).inPath("result.tools[*].name").isArray().contains("list_topics");
        assertThatJson(toolsResp.body()).inPath("result.tools[?(@.name=='list_topics')].outputSchema")
                .isEqualTo(json("""
                        [{
                          "type": "object",
                          "properties": {
                            "virtualTopics": {
                              "type": "array",
                              "items": {
                                "type": "object",
                                "properties": {
                                  "name": {"type": "string"},
                                  "physicalTopic": {"type": "string"},
                                  "partitions": {"type": "integer"},
                                  "replicationFactor": {"type": "integer"},
                                  "filter": {
                                    "type": "object",
                                    "properties": {
                                      "type": {"type": "string"},
                                      "description": {"type": "string"}
                                    },
                                    "required": ["type", "description"],
                                    "additionalProperties": false
                                  },
                                  "valueFormat": {"type": "string", "enum": ["json", "avro", "raw"]},
                                  "exposesPhysicalTopic": {"type": "boolean"}
                                },
                                "required": ["name", "physicalTopic", "partitions", "replicationFactor", "valueFormat", "exposesPhysicalTopic"],
                                "additionalProperties": false
                              }
                            },
                            "physicalTopics": {
                              "type": "array",
                              "items": {
                                "type": "object",
                                "properties": {
                                  "name": {"type": "string"},
                                  "partitions": {"type": "integer"},
                                  "replicationFactor": {"type": "integer"}
                                },
                                "required": ["name", "partitions", "replicationFactor"],
                                "additionalProperties": false
                              }
                            }
                          },
                          "required": ["virtualTopics", "physicalTopics"],
                          "additionalProperties": false
                        }]
                        """));
    }

    @Test
    void returnsVirtualAndPhysicalTopicsAsStructuredContentAndText() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2", new HeaderEqualsFilterConfig("region", "eu"), true,
                        new JsonFormatConfig())));
        cache = cacheWith(topic("orders-v2", 3, 2));
        startServer();

        // when
        var listTopicsResp = callTool("list_topics");

        // then
        assertThat(listTopicsResp).hasStatusCode(200);
        assertThatJson(listTopicsResp.body()).inPath("result.isError").isEqualTo(false);
        assertThatJson(listTopicsResp.body()).inPath("result.structuredContent").isEqualTo("""
                {
                  "virtualTopics": [{
                    "name": "orders",
                    "physicalTopic": "orders-v2",
                    "partitions": 3,
                    "replicationFactor": 2,
                    "filter": {"type": "headerEquals", "description": "header 'region' equals 'eu'"},
                    "valueFormat": "json",
                    "exposesPhysicalTopic": true
                  }],
                  "physicalTopics": [{"name": "orders-v2", "partitions": 3, "replicationFactor": 2}]
                }""");
        assertThatJson(listTopicsResp.body()).inPath("result.content[0].text").isString()
                .satisfies(text -> assertThatJson(text)
                        .withFailMessage(() -> "the text content is not the structured content as JSON: " + text)
                        .isEqualTo(json(structuredContent(listTopicsResp.body()))));
    }

    @Test
    void omitsTheFilterOfAnUnfilteredVirtualTopic() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of("orders", new VirtualTopicConfig("orders-v2")));
        startServer();

        // when
        var listTopicsResp = callTool("list_topics");

        // then
        assertThatJson(listTopicsResp.body()).inPath("result.structuredContent.virtualTopics[0]").isEqualTo("""
                {
                  "name": "orders",
                  "physicalTopic": "orders-v2",
                  "partitions": 0,
                  "replicationFactor": 0,
                  "valueFormat": "raw",
                  "exposesPhysicalTopic": false
                }""");
    }

    @Test
    void nameMatchesAnywhereInTheVirtualAndPhysicalTopicNames() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2"),
                "payments", new VirtualTopicConfig("payments-v1")));
        cache = cacheWith(topic("orders-v2", 1, 1), topic("payments-v1", 1, 1));
        startServer();

        // when
        var listTopicsResp = listTopics("ders");

        // then
        assertThatJson(listTopicsResp.body()).inPath("result.isError").isEqualTo(false);
        assertThatJson(listTopicsResp.body()).inPath("result.structuredContent.virtualTopics[*].name")
                .isArray().containsExactly("orders");
        assertThatJson(listTopicsResp.body()).inPath("result.structuredContent.physicalTopics[*].name")
                .isArray().containsExactly("orders-v2");
    }

    @Test
    void namePatternMatchesPhysicalTopicsByTheirPhysicalName() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2"),
                "payments", new VirtualTopicConfig("payments-v1")));
        cache = cacheWith(topic("orders-v2", 1, 1), topic("payments-v1", 1, 1));
        startServer();

        // when
        var listTopicsResp = listTopics("^payments-v[0-9]$");

        // then
        assertThatJson(listTopicsResp.body()).inPath("result.structuredContent.virtualTopics").isEqualTo("[]");
        assertThatJson(listTopicsResp.body()).inPath("result.structuredContent.physicalTopics[*].name")
                .isArray().containsExactly("payments-v1");
    }

    @Test
    void blankNameListsEveryTopic() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2"),
                "payments", new VirtualTopicConfig("payments-v1")));
        startServer();

        // when
        var listTopicsResp = listTopics("   ");

        // then
        assertThatJson(listTopicsResp.body()).inPath("result.structuredContent.virtualTopics[*].name")
                .isArray().containsExactlyInAnyOrder("orders", "payments");
    }

    @Test
    void aNameMatchingNothingReturnsEmptyListsWithoutAToolError() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of("orders", new VirtualTopicConfig("orders-v2")));
        cache = cacheWith(topic("orders-v2", 1, 1));
        startServer();

        // when
        var listTopicsResp = listTopics("nomatch");

        // then
        assertThatJson(listTopicsResp.body()).inPath("result.isError").isEqualTo(false);
        assertThatJson(listTopicsResp.body()).inPath("result.structuredContent")
                .isEqualTo("{\"virtualTopics\":[],\"physicalTopics\":[]}");
    }

    @Test
    void anInvalidRegularExpressionBecomesAToolError() throws Exception {
        // given
        startServer();

        // when
        var listTopicsResp = listTopics("(");

        // then
        assertThatJson(listTopicsResp.body()).inPath("result.isError").isEqualTo(true);
        assertThatJson(listTopicsResp.body()).inPath("result.content[0].text")
                .isEqualTo("name is not a valid regular expression: Unclosed group");
    }

    @Test
    void aPatternLongerThanTheLimitBecomesAToolError() throws Exception {
        // given
        startServer();

        // when
        var listTopicsResp = listTopics("a".repeat(ListTopicsMcpTool.MAX_NAME_PATTERN_LENGTH + 1));

        // then
        assertThatJson(listTopicsResp.body()).inPath("result.isError").isEqualTo(true);
        assertThatJson(listTopicsResp.body()).inPath("result.content[0].text")
                .isEqualTo("name must be at most " + ListTopicsMcpTool.MAX_NAME_PATTERN_LENGTH + " characters");
    }

    @Test
    void typeVirtualListsOnlyTheVirtualTopics() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2"),
                "payments", new VirtualTopicConfig("payments-v1")));
        cache = cacheWith(topic("orders-v2", 1, 1), topic("payments-v1", 1, 1));
        startServer();

        // when
        var listTopicsResp = callToolWithArgs("list_topics", "{\"type\":\"virtual\"}");

        // then
        assertThatJson(listTopicsResp.body()).inPath("result.isError").isEqualTo(false);
        assertThatJson(listTopicsResp.body()).inPath("result.structuredContent.virtualTopics[*].name")
                .isArray().containsExactlyInAnyOrder("orders", "payments");
        assertThatJson(listTopicsResp.body()).inPath("result.structuredContent.physicalTopics").isEqualTo("[]");
    }

    @Test
    void typePhysicalListsOnlyTheBrokerTopics() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2"),
                "payments", new VirtualTopicConfig("payments-v1")));
        cache = cacheWith(topic("orders-v2", 1, 1), topic("payments-v1", 1, 1));
        startServer();

        // when
        var listTopicsResp = callToolWithArgs("list_topics", "{\"type\":\"physical\"}");

        // then
        assertThatJson(listTopicsResp.body()).inPath("result.isError").isEqualTo(false);
        assertThatJson(listTopicsResp.body()).inPath("result.structuredContent.virtualTopics").isEqualTo("[]");
        assertThatJson(listTopicsResp.body()).inPath("result.structuredContent.physicalTopics[*].name")
                .isArray().containsExactly("orders-v2", "payments-v1");
    }

    @Test
    void typeAndNameFilterInsideTheChosenLayer() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2"),
                "payments", new VirtualTopicConfig("payments-v1")));
        cache = cacheWith(topic("orders-v2", 1, 1), topic("payments-v1", 1, 1));
        startServer();

        // when
        var listTopicsResp = callToolWithArgs("list_topics", "{\"type\":\"physical\",\"name\":\"ders\"}");

        // then
        assertThatJson(listTopicsResp.body()).inPath("result.structuredContent.virtualTopics").isEqualTo("[]");
        assertThatJson(listTopicsResp.body()).inPath("result.structuredContent.physicalTopics[*].name")
                .isArray().containsExactly("orders-v2");
    }

    @Test
    void anUnknownTypeBecomesAToolError() throws Exception {
        // given
        startServer();

        // when
        var listTopicsResp = callToolWithArgs("list_topics", "{\"type\":\"both\"}");

        // then
        assertThatJson(listTopicsResp.body()).inPath("result.isError").isEqualTo(true);
        assertThatJson(listTopicsResp.body()).inPath("result.content[0].text")
                .isEqualTo("type must be 'virtual' or 'physical'");
    }

    @Test
    void topicServiceFailureBecomesAGenericToolError() throws Exception {
        // given
        topicService = mock(TopicService.class);
        when(topicService.listTopics()).thenThrow(new IllegalStateException("metadata cache exploded"));
        startServer();

        // when
        var listTopicsResp = callTool("list_topics");

        // then
        assertThat(listTopicsResp).hasStatusCode(200);
        assertThatJson(listTopicsResp.body()).inPath("result.isError").isEqualTo(true);
        assertThatJson(listTopicsResp.body()).inPath("result.content[0].text").isEqualTo(McpTool.INTERNAL_ERROR);
    }

    @Test
    void logsOneLineWithCallerAndToolButNotTheArguments() throws Exception {
        // given
        startServer();
        var logger = (Logger) LoggerFactory.getLogger(McpTool.class);
        var logs = new ListAppender<ILoggingEvent>();
        logs.start();
        logger.addAppender(logs);

        // when
        try {
            rpc("tools/call", """
                    {"name":"list_topics","arguments":{"secret":"s3cr3t-value"}}""");
        } finally {
            logger.detachAppender(logs);
        }

        // then
        var lines = logs.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        assertThat(logs.list.stream().filter(event -> event.getLevel() == Level.INFO).map(ILoggingEvent::getFormattedMessage))
                .withFailMessage(() -> "the call was not logged once with the 'none' provider's admin and no client: " + lines)
                .containsExactly("MCP tool list_topics called by admin via -: ok");
        assertThat(lines)
                .withFailMessage(() -> "the tool arguments were logged: " + lines)
                .noneMatch(line -> line.contains("s3cr3t-value"));
    }

    /// Calls `list_topics` with the `name` argument. The name must not contain quotes or backslashes.
    private HttpResponse<String> listTopics(String name) throws Exception {
        return callToolWithArgs("list_topics", "{\"name\":\"" + name + "\"}");
    }
}
