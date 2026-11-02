package io.jonasg.kawa.mcp;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.json;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/// Slice tests for [ListTopicsTool]: real JSON-RPC requests to `POST /mcp` on a booted admin server.
class ListTopicsToolSliceTest extends McpSliceTestBase {

    @Test
    void advertisesListTopicsWithItsOutputSchema() throws Exception {
        // given
        startServer();

        // when
        var toolsResp = rpc("tools/list", "{}");

        // then
        assertThat(toolsResp).hasStatusCode(200);
        assertThatJson(toolsResp.body()).inPath("result.tools[*].name").isArray().containsExactly("list_topics");
        assertThatJson(toolsResp.body()).inPath("result.tools[0].outputSchema").isEqualTo(json(
                new JsonMapper().writeValueAsString(TopicsView.SCHEMA)));
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
        assertThatJson(listTopicsResp.body()).inPath("result.content[0].text").isEqualTo(ToolResults.INTERNAL_ERROR);
    }

    @Test
    void logsOneLineWithCallerAndToolButNotTheArguments() throws Exception {
        // given
        startServer();
        var logger = (Logger) LoggerFactory.getLogger(ToolResults.class);
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

    private static String structuredContent(String body) {
        var mapper = new JsonMapper();
        return mapper.writeValueAsString(mapper.readTree(body).get("result").get("structuredContent"));
    }
}
