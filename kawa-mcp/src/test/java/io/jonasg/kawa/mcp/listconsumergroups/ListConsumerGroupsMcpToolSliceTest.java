package io.jonasg.kawa.mcp.listconsumergroups;

import io.jonasg.kawa.broker.BrokerAdmin.GroupDescription;
import io.jonasg.kawa.broker.BrokerAdmin.GroupMember;
import io.jonasg.kawa.broker.BrokerAdmin.PartitionId;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.mcp.McpSliceTestBase;
import io.jonasg.kawa.mcp.McpTool;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import net.javacrumbs.jsonunit.core.Option;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.json;

/// Slice tests for `list_consumer_groups`: real JSON-RPC requests to `POST /mcp` on a booted admin server,
/// with the consumer groups set up on the fake broker.
class ListConsumerGroupsMcpToolSliceTest extends McpSliceTestBase {

    private static final String TOOL = "list_consumer_groups";

    @Test
    void advertisesListConsumerGroupsWithItsOutputSchema() throws Exception {
        // given
        startServer();

        // when
        var toolsResp = rpc("tools/list", "{}");

        // then
        assertThat(toolsResp).hasStatusCode(200);
        assertThatJson(toolsResp.body()).inPath("result.tools[*].name")
                .withFailMessage(() -> "list_consumer_groups was not advertised by tools/list: " + toolsResp.body())
                .isArray().contains(TOOL);
        assertThatJson(toolsResp.body()).inPath("result.tools[?(@.name=='list_consumer_groups')].outputSchema")
                .withFailMessage(() -> "list_consumer_groups did not advertise the output schema of its groups: " + toolsResp.body())
                .isEqualTo(json("""
                        [{
                          "type": "object",
                          "properties": {
                            "groups": {
                              "type": "array",
                              "items": {
                                "type": "object",
                                "properties": {
                                  "groupId": {"type": "string"},
                                  "type": {"type": "string"},
                                  "state": {"type": "string"},
                                  "protocolType": {"type": "string"},
                                  "lagApplicable": {"type": "boolean"},
                                  "totalLag": {"type": "integer"},
                                  "uncommittedPartitions": {"type": "integer"},
                                  "lagIsUpperBound": {"type": "boolean"}
                                },
                                "required": ["groupId", "type", "state", "lagApplicable"],
                                "additionalProperties": false
                              }
                            }
                          },
                          "required": ["groups"],
                          "additionalProperties": false
                        }]
                        """));
    }

    @Test
    void listsEveryGroupWithItsStateAndTotalLag() throws Exception {
        // given
        groupReading("orders-consumers", "Stable", new PartitionId("orders-v2", 0));
        commit("orders-consumers", new PartitionId("orders-v2", 0), 40);
        end(new PartitionId("orders-v2", 0), 100);
        groupReading("audit", "Empty");
        startServer();

        // when
        var listResp = callTool(TOOL);

        // then
        assertThat(listResp).hasStatusCode(200);
        assertThatJson(listResp.body()).inPath("result.isError")
                .withFailMessage(() -> "list_consumer_groups was a tool error: " + listResp.body()).isEqualTo(false);
        assertThatJson(structuredContent(listResp.body()))
                .when(Option.IGNORING_ARRAY_ORDER)
                .withFailMessage(() -> "list_consumer_groups did not list both groups with their state and total lag: " + listResp.body())
                .isEqualTo(json("""
                        {
                          "groups": [
                            {"groupId": "orders-consumers", "type": "CLASSIC", "state": "Stable", "protocolType": "consumer",
                             "lagApplicable": true, "totalLag": 60, "uncommittedPartitions": 0, "lagIsUpperBound": false},
                            {"groupId": "audit", "type": "CLASSIC", "state": "Empty", "protocolType": "consumer",
                             "lagApplicable": true, "totalLag": 0, "uncommittedPartitions": 0, "lagIsUpperBound": false}
                          ]
                        }"""));
        assertThatJson(listResp.body()).inPath("result.content[0].text").isString()
                .withFailMessage(() -> "the text content does not carry the same groups as the structured content: " + listResp.body())
                .satisfies(text -> assertThatJson(text)
                        .when(Option.IGNORING_ARRAY_ORDER)
                        .isEqualTo(json(structuredContent(listResp.body()))));
    }

    @Test
    void countsPartitionsWithoutACommitInsteadOfAddingThemToTheLag() throws Exception {
        // given: one partition committed and 60 behind, one assigned partition the group never committed
        groupReading("orders-consumers", "Stable", new PartitionId("orders-v2", 0), new PartitionId("orders-v2", 1));
        commit("orders-consumers", new PartitionId("orders-v2", 0), 40);
        end(new PartitionId("orders-v2", 0), 100);
        end(new PartitionId("orders-v2", 1), 70);
        startServer();

        // when
        var listResp = callTool(TOOL);

        // then
        assertThatJson(listResp.body()).inPath("result.structuredContent.groups[0].totalLag")
                .withFailMessage(() -> "the partition without a commit was added to the total lag: " + listResp.body())
                .isEqualTo(60);
        assertThatJson(listResp.body()).inPath("result.structuredContent.groups[0].uncommittedPartitions")
                .withFailMessage(() -> "the partition without a commit was not counted as uncommitted: " + listResp.body())
                .isEqualTo(1);
    }

    @Test
    void marksTotalLagAsUpperBoundWhenTheGroupReadsAFilteredVirtualTopic() throws Exception {
        // given: 'orders-acme' is filtered, 'orders' is not
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2"),
                "orders-acme", new VirtualTopicConfig("orders-acme-v2", new HeaderEqualsFilterConfig("tenant", "acme"))));
        groupReading("acme-readers", "Stable", new PartitionId("orders-acme-v2", 0));
        commit("acme-readers", new PartitionId("orders-acme-v2", 0), 40);
        end(new PartitionId("orders-acme-v2", 0), 100);
        groupReading("orders-readers", "Stable", new PartitionId("orders-v2", 0));
        commit("orders-readers", new PartitionId("orders-v2", 0), 40);
        end(new PartitionId("orders-v2", 0), 100);
        startServer();

        // when
        var listResp = callTool(TOOL);

        // then
        assertThatJson(listResp.body()).inPath("result.structuredContent.groups[?(@.groupId=='acme-readers')].lagIsUpperBound")
                .withFailMessage(() -> "lag through a filtered virtual topic was not marked as an upper bound: " + listResp.body())
                .isEqualTo(json("[true]"));
        assertThatJson(listResp.body()).inPath("result.structuredContent.groups[?(@.groupId=='orders-readers')].lagIsUpperBound")
                .withFailMessage(() -> "lag through an unfiltered virtual topic was marked as an upper bound: " + listResp.body())
                .isEqualTo(json("[false]"));
    }

    @Test
    void answersAnEmptyListWhenNoGroupExists() throws Exception {
        // given
        startServer();

        // when
        var listResp = callTool(TOOL);

        // then
        assertThatJson(listResp.body()).inPath("result.isError")
                .withFailMessage(() -> "list_consumer_groups with no groups was a tool error: " + listResp.body()).isEqualTo(false);
        assertThatJson(listResp.body()).inPath("result.structuredContent")
                .withFailMessage(() -> "list_consumer_groups with no groups did not answer an empty list: " + listResp.body())
                .isEqualTo(json("{\"groups\": []}"));
    }

    @Test
    void answersAToolErrorWhenTheBrokerCannotBeRead() throws Exception {
        // given
        brokerAdmin.consumerGroupError = new IllegalStateException("broker unreachable");
        startServer();

        // when
        var listResp = callTool(TOOL);

        // then
        assertThatJson(listResp.body()).inPath("result.isError")
                .withFailMessage(() -> "a broker that cannot be read was not a tool error: " + listResp.body()).isEqualTo(true);
        assertThatJson(listResp.body()).inPath("result.content[0].text")
                .withFailMessage(() -> "the broker failure was not answered with the generic internal error: " + listResp.body())
                .isEqualTo(McpTool.INTERNAL_ERROR);
    }

    @Test
    void listsAShareGroupWithLagNotApplicable() throws Exception {
        // given
        brokerAdmin.groups.put("orders-share", new GroupDescription("orders-share", "SHARE", "Stable", List.of()));
        startServer();

        // when
        var listResp = callTool(TOOL);

        // then
        assertThat(listResp).hasStatusCode(200);
        assertThatJson(listResp.body()).inPath("result.isError")
                .withFailMessage(() -> "list_consumer_groups was a tool error: " + listResp.body()).isEqualTo(false);
        assertThatJson(structuredContent(listResp.body()))
                .withFailMessage(() -> "the share group was not listed with its type and state and no lag: " + listResp.body())
                .isEqualTo(json("""
                        {
                          "groups": [
                            {"groupId": "orders-share", "type": "SHARE", "state": "Stable", "lagApplicable": false}
                          ]
                        }"""));
    }

    /// Adds a group with one member assigned `partitions`, or no member at all when none are given.
    private void groupReading(String groupId, String state, PartitionId... partitions) {
        List<GroupMember> members = partitions.length == 0
                ? List.of()
                : List.of(new GroupMember("member-1", "client-1", "/10.0.0.1", List.of(partitions)));
        brokerAdmin.groups.put(groupId, new GroupDescription(groupId, "CLASSIC", state, members));
    }

    private void commit(String groupId, PartitionId partition, long offset) {
        brokerAdmin.committed.computeIfAbsent(groupId, _ -> new HashMap<>()).put(partition, offset);
    }

    private void end(PartitionId partition, long offset) {
        brokerAdmin.latest.put(partition, offset);
    }
}
