package io.jonasg.kawa.mcp;

import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.http.topics.TopicAdmin.GroupDetails;
import io.jonasg.kawa.http.topics.TopicAdmin.GroupMember;
import io.jonasg.kawa.http.topics.TopicAdmin.GroupPartition;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.json;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.when;

/// Slice tests for [DescribeConsumerGroupTool]: real JSON-RPC requests to `POST /mcp` on a booted
/// admin server, with the broker admin stubbed.
class DescribeConsumerGroupToolSliceTest extends McpSliceTestBase {

    private static final GroupPartition ORDERS_0 = new GroupPartition("orders-v2", 0);
    private static final GroupPartition ORDERS_1 = new GroupPartition("orders-v2", 1);

    @Test
    void advertisesBothConsumerGroupToolsWithTheirOutputSchemas() throws Exception {
        // given
        startServer();

        // when
        var toolsResp = rpc("tools/list", "{}");

        // then
        assertThatJson(toolsResp.body()).inPath("result.tools[*].name").isArray()
                .contains(ListConsumerGroupsTool.NAME, DescribeConsumerGroupTool.NAME);
        assertThatJson(toolsResp.body()).inPath("result.tools[?(@.name == 'describe_consumer_group')].outputSchema")
                .isArray().containsExactly(json(new JsonMapper().writeValueAsString(ConsumerGroupView.SCHEMA)));
        assertThatJson(toolsResp.body()).inPath("result.tools[?(@.name == 'list_consumer_groups')].outputSchema")
                .isArray().containsExactly(json(new JsonMapper().writeValueAsString(ConsumerGroupsView.SCHEMA)));
    }

    @Test
    void describesStateMembersAndPerPartitionLagAgainstThePhysicalTopic() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of("orders", new VirtualTopicConfig("orders-v2")));
        stubGroup(Map.of(ORDERS_0, 10L, ORDERS_1, 3L), Map.of(ORDERS_0, 15L, ORDERS_1, 3L));
        startServer();

        // when
        var describeResp = describe("analytics");

        // then
        assertThat(describeResp).hasStatusCode(200);
        assertThatJson(describeResp.body()).inPath("result.isError").isEqualTo(false);
        assertThatJson(describeResp.body()).inPath("result.structuredContent").isEqualTo("""
                {
                  "groupId": "analytics",
                  "state": "Stable",
                  "totalLag": 5,
                  "lagIsUpperBound": false,
                  "members": [{
                    "memberId": "consumer-1-abc",
                    "clientId": "consumer-1",
                    "host": "/10.0.0.7",
                    "assignment": [{"topic": "orders-v2", "partitions": [0, 1]}]
                  }],
                  "topics": [{
                    "physicalTopic": "orders-v2",
                    "virtualTopics": [{"name": "orders"}],
                    "totalLag": 5,
                    "lagIsUpperBound": false,
                    "partitions": [
                      {"partition": 0, "committedOffset": 10, "endOffset": 15, "lag": 5},
                      {"partition": 1, "committedOffset": 3, "endOffset": 3, "lag": 0}
                    ]
                  }]
                }""");
        String text = textContent(describeResp);
        assertThatJson(text)
                .withFailMessage(() -> "the text content is not the structured content as JSON: " + text)
                .isEqualTo(json(new JsonMapper().writeValueAsString(
                        new JsonMapper().readTree(describeResp.body()).get("result").get("structuredContent"))));
    }

    @Test
    void anAssignedPartitionWithoutACommitHasNoCommittedOffsetInsteadOfZero() throws Exception {
        // given
        stubGroup(Map.of(ORDERS_0, 10L), Map.of(ORDERS_0, 15L, ORDERS_1, 8L));
        startServer();

        // when
        var describeResp = describe("analytics");

        // then
        assertThatJson(describeResp.body()).inPath("result.structuredContent.topics[0].partitions").isEqualTo("""
                [
                  {"partition": 0, "committedOffset": 10, "endOffset": 15, "lag": 5},
                  {"partition": 1, "endOffset": 8}
                ]""");
        assertThatJson(describeResp.body()).inPath("result.structuredContent.totalLag").isEqualTo(5);
    }

    @Test
    void marksTheLagAsAnUpperBoundWhenTheVirtualTopicFilters() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2", new HeaderEqualsFilterConfig("region", "eu"), false, null)));
        stubGroup(Map.of(ORDERS_0, 10L, ORDERS_1, 3L), Map.of(ORDERS_0, 15L, ORDERS_1, 3L));
        startServer();

        // when
        var describeResp = describe("analytics");

        // then
        assertThatJson(describeResp.body()).inPath("result.structuredContent.lagIsUpperBound").isEqualTo(true);
        assertThatJson(describeResp.body()).inPath("result.structuredContent.topics[0].lagIsUpperBound").isEqualTo(true);
        assertThatJson(describeResp.body()).inPath("result.structuredContent.topics[0].virtualTopics").isEqualTo("""
                [{"name": "orders", "filter": {"type": "headerEquals", "description": "header 'region' equals 'eu'"}}]""");
    }

    @Test
    void anUnknownGroupBecomesAToolErrorNamingIt() throws Exception {
        // given
        when(topicAdmin.describeConsumerGroups(anyCollection())).thenReturn(Map.of());
        startServer();

        // when
        var describeResp = describe("ghost");

        // then
        assertThatJson(describeResp.body()).inPath("result.isError").isEqualTo(true);
        assertThatJson(describeResp.body()).inPath("result.content[0].text").isEqualTo("consumer group 'ghost' not found");
    }

    @Test
    void aBlankGroupIdBecomesAToolError() throws Exception {
        // given
        startServer();

        // when
        var describeResp = describe("  ");

        // then
        assertThatJson(describeResp.body()).inPath("result.isError").isEqualTo(true);
        assertThatJson(describeResp.body()).inPath("result.content[0].text").isEqualTo("groupId must be a non-blank string");
    }

    /// Stubs the group `analytics`, one member assigned partitions 0 and 1 of `orders-v2`.
    private void stubGroup(Map<GroupPartition, Long> committed, Map<GroupPartition, Long> ends) throws Exception {
        var member = new GroupMember("consumer-1-abc", "consumer-1", "/10.0.0.7", List.of(ORDERS_0, ORDERS_1));
        when(topicAdmin.describeConsumerGroups(anyCollection()))
                .thenReturn(Map.of("analytics", new GroupDetails("analytics", "Stable", List.of(member))));
        when(topicAdmin.committedOffsets(anyCollection())).thenReturn(Map.of("analytics", committed));
        when(topicAdmin.endOffsets(anyCollection())).thenReturn(ends);
    }

    private HttpResponse<String> describe(String groupId) throws Exception {
        return rpc("tools/call", "{\"name\":\"" + DescribeConsumerGroupTool.NAME + "\",\"arguments\":{\"groupId\":\"" + groupId + "\"}}");
    }

    private static String textContent(HttpResponse<String> response) {
        return new JsonMapper().readTree(response.body()).get("result").get("content").get(0).get("text").asString();
    }
}
