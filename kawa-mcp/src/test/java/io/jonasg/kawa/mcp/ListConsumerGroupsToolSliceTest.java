package io.jonasg.kawa.mcp;

import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.http.topics.TopicAdmin.GroupPartition;
import io.jonasg.kawa.http.topics.TopicAdmin.GroupSummary;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/// Slice tests for [ListConsumerGroupsTool]: real JSON-RPC requests to `POST /mcp` on a booted admin
/// server, with the broker admin stubbed.
class ListConsumerGroupsToolSliceTest extends McpSliceTestBase {

    private static final GroupPartition ORDERS_0 = new GroupPartition("orders-v2", 0);
    private static final GroupPartition ORDERS_1 = new GroupPartition("orders-v2", 1);
    private static final GroupPartition PAYMENTS_0 = new GroupPartition("payments", 0);

    @Test
    void listsEveryGroupWithItsStateAndTotalLag() throws Exception {
        // given
        when(topicAdmin.listConsumerGroups()).thenReturn(List.of(
                new GroupSummary("billing", "Empty"),
                new GroupSummary("analytics", "Stable")));
        when(topicAdmin.committedOffsets(anyCollection())).thenReturn(Map.of(
                "analytics", Map.of(ORDERS_0, 10L, ORDERS_1, 5L),
                "billing", Map.of(PAYMENTS_0, 7L)));
        when(topicAdmin.endOffsets(anyCollection())).thenReturn(Map.of(ORDERS_0, 15L, ORDERS_1, 5L, PAYMENTS_0, 9L));
        startServer();

        // when
        var listResp = callTool(ListConsumerGroupsTool.NAME);

        // then
        assertThat(listResp).hasStatusCode(200);
        assertThatJson(listResp.body()).inPath("result.isError").isEqualTo(false);
        assertThatJson(listResp.body()).inPath("result.structuredContent").isEqualTo("""
                {"groups": [
                  {"groupId": "analytics", "state": "Stable", "totalLag": 5, "lagIsUpperBound": false},
                  {"groupId": "billing", "state": "Empty", "totalLag": 2, "lagIsUpperBound": false}
                ]}""");
    }

    @Test
    void readsCommittedOffsetsOfAllGroupsInOneCall() throws Exception {
        // given
        when(topicAdmin.listConsumerGroups()).thenReturn(List.of(
                new GroupSummary("analytics", "Stable"),
                new GroupSummary("billing", "Empty")));
        when(topicAdmin.committedOffsets(anyCollection())).thenReturn(Map.of());
        when(topicAdmin.endOffsets(anyCollection())).thenReturn(Map.of());
        startServer();

        // when
        callTool(ListConsumerGroupsTool.NAME);

        // then
        verify(topicAdmin).committedOffsets(List.of("analytics", "billing"));
    }

    @Test
    void marksTheLagAsAnUpperBoundWhenAVirtualTopicFiltersTheTopic() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2", new HeaderEqualsFilterConfig("region", "eu"), false, null)));
        when(topicAdmin.listConsumerGroups()).thenReturn(List.of(new GroupSummary("analytics", "Stable")));
        when(topicAdmin.committedOffsets(anyCollection())).thenReturn(Map.of("analytics", Map.of(ORDERS_0, 10L)));
        when(topicAdmin.endOffsets(anyCollection())).thenReturn(Map.of(ORDERS_0, 15L));
        startServer();

        // when
        var listResp = callTool(ListConsumerGroupsTool.NAME);

        // then
        assertThatJson(listResp.body()).inPath("result.structuredContent.groups[0].lagIsUpperBound").isEqualTo(true);
    }

    @Test
    void aBrokerFailureBecomesAGenericToolError() throws Exception {
        // given
        when(topicAdmin.listConsumerGroups()).thenThrow(new IllegalStateException("coordinator unavailable"));
        startServer();

        // when
        var listResp = callTool(ListConsumerGroupsTool.NAME);

        // then
        assertThatJson(listResp.body()).inPath("result.isError").isEqualTo(true);
        assertThatJson(listResp.body()).inPath("result.content[0].text").isEqualTo(ToolResults.INTERNAL_ERROR);
    }
}
