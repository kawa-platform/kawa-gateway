package io.jonasg.kawa.mcp.describeconsumergroup;

import io.jonasg.kawa.broker.BrokerAdmin.GroupDescription;
import io.jonasg.kawa.broker.BrokerAdmin.GroupMember;
import io.jonasg.kawa.broker.BrokerAdmin.PartitionId;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.mcp.McpSliceTestBase;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.json;

/// Slice tests for `describe_consumer_group`: real JSON-RPC requests to `POST /mcp` on a booted admin server,
/// with the consumer groups set up on the fake broker.
class DescribeConsumerGroupMcpToolSliceTest extends McpSliceTestBase {

    private static final String TOOL = "describe_consumer_group";

    @Test
    void advertisesDescribeConsumerGroupWithItsInputAndOutputSchema() throws Exception {
        // given
        startServer();

        // when
        var toolsResp = rpc("tools/list", "{}");

        // then
        assertThat(toolsResp).hasStatusCode(200);
        assertThatJson(toolsResp.body()).inPath("result.tools[*].name")
                .withFailMessage(() -> "describe_consumer_group was not advertised by tools/list: " + toolsResp.body())
                .isArray().contains(TOOL);
        assertThatJson(toolsResp.body()).inPath("result.tools[?(@.name=='describe_consumer_group')].inputSchema")
                .withFailMessage(() -> "describe_consumer_group did not advertise its required groupId input: " + toolsResp.body())
                .isEqualTo(json("""
                        [{
                          "type": "object",
                          "properties": {
                            "groupId": {"type": "string"}
                          },
                          "required": ["groupId"]
                        }]
                        """));
        assertThatJson(toolsResp.body()).inPath("result.tools[?(@.name=='describe_consumer_group')].outputSchema")
                .withFailMessage(() -> "describe_consumer_group did not advertise the output schema of its group: " + toolsResp.body())
                .isEqualTo(json("""
                        [{
                          "type": "object",
                          "properties": {
                            "groupId": {"type": "string"},
                            "type": {"type": "string"},
                            "state": {"type": "string"},
                            "lagApplicable": {"type": "boolean"},
                            "members": {
                              "type": "array",
                              "items": {
                                "type": "object",
                                "properties": {
                                  "memberId": {"type": "string"},
                                  "clientId": {"type": "string"},
                                  "host": {"type": "string"},
                                  "assignedPartitions": {
                                    "type": "array",
                                    "items": {
                                      "type": "object",
                                      "properties": {
                                        "topic": {"type": "string"},
                                        "partition": {"type": "integer"}
                                      },
                                      "required": ["topic", "partition"],
                                      "additionalProperties": false
                                    }
                                  }
                                },
                                "required": ["memberId", "clientId", "host", "assignedPartitions"],
                                "additionalProperties": false
                              }
                            },
                            "topics": {
                              "type": "array",
                              "items": {
                                "type": "object",
                                "properties": {
                                  "physicalTopic": {"type": "string"},
                                  "virtualTopics": {
                                    "type": "array",
                                    "items": {"type": "string"}
                                  },
                                  "lagIsUpperBound": {"type": "boolean"},
                                  "partitions": {
                                    "type": "array",
                                    "items": {
                                      "type": "object",
                                      "properties": {
                                        "partition": {"type": "integer"},
                                        "committedOffset": {"type": "integer"},
                                        "endOffset": {"type": "integer"},
                                        "lag": {"type": "integer"}
                                      },
                                      "required": ["partition"],
                                      "additionalProperties": false
                                    }
                                  }
                                },
                                "required": ["physicalTopic", "virtualTopics", "lagIsUpperBound", "partitions"],
                                "additionalProperties": false
                              }
                            },
                            "totalLag": {"type": "integer"},
                            "uncommittedPartitions": {"type": "integer"}
                          },
                          "required": ["groupId", "type", "state", "lagApplicable"],
                          "additionalProperties": false
                        }]
                        """));
    }

    @Test
    void describesAGroupWithItsStateAndMembers() throws Exception {
        // given
        groupReading("orders-consumers", "Stable",
                new Member("member-1", "client-1", "/10.0.0.1", new PartitionId("orders-v2", 0)),
                new Member("member-2", "client-2", "/10.0.0.2", new PartitionId("orders-v2", 1)));
        startServer();

        // when
        var describeResp = callToolWithArgs(TOOL, "{\"groupId\": \"orders-consumers\"}");

        // then
        assertThat(describeResp).hasStatusCode(200);
        assertThatJson(describeResp.body()).inPath("result.isError")
                .withFailMessage(() -> "describe_consumer_group was a tool error: " + describeResp.body()).isEqualTo(false);
        assertThatJson(describeResp.body()).inPath("result.structuredContent.groupId")
                .withFailMessage(() -> "describe_consumer_group did not describe 'orders-consumers': " + describeResp.body())
                .isEqualTo("orders-consumers");
        assertThatJson(describeResp.body()).inPath("result.structuredContent.type")
                .withFailMessage(() -> "describe_consumer_group did not report the group type: " + describeResp.body())
                .isEqualTo("CLASSIC");
        assertThatJson(describeResp.body()).inPath("result.structuredContent.state")
                .withFailMessage(() -> "describe_consumer_group did not report the group state: " + describeResp.body())
                .isEqualTo("Stable");
        assertThatJson(describeResp.body()).inPath("result.structuredContent.lagApplicable")
                .withFailMessage(() -> "describe_consumer_group did not report that the lag of a classic group applies: " + describeResp.body())
                .isEqualTo(true);
        assertThatJson(describeResp.body()).inPath("result.structuredContent.members")
                .withFailMessage(() -> "describe_consumer_group did not list each member with its assigned partitions: " + describeResp.body())
                .isEqualTo(json("""
                        [
                          {"memberId": "member-1", "clientId": "client-1", "host": "/10.0.0.1",
                           "assignedPartitions": [{"topic": "orders-v2", "partition": 0}]},
                          {"memberId": "member-2", "clientId": "client-2", "host": "/10.0.0.2",
                           "assignedPartitions": [{"topic": "orders-v2", "partition": 1}]}
                        ]"""));
    }

    @Test
    void reportsCommittedOffsetEndOffsetAndLagForEachPartitionTheGroupReads() throws Exception {
        // given: partition 0 is 60 behind, partition 1 is 45 behind
        groupReading("orders-consumers", "Stable",
                new Member("member-1", "client-1", "/10.0.0.1", new PartitionId("orders-v2", 0), new PartitionId("orders-v2", 1)));
        commit("orders-consumers", new PartitionId("orders-v2", 0), 40);
        commit("orders-consumers", new PartitionId("orders-v2", 1), 25);
        end(new PartitionId("orders-v2", 0), 100);
        end(new PartitionId("orders-v2", 1), 70);
        startServer();

        // when
        var describeResp = callToolWithArgs(TOOL, "{\"groupId\": \"orders-consumers\"}");

        // then
        assertThat(describeResp).hasStatusCode(200);
        assertThatJson(describeResp.body()).inPath("result.isError")
                .withFailMessage(() -> "describe_consumer_group was a tool error: " + describeResp.body()).isEqualTo(false);
        assertThatJson(describeResp.body()).inPath("result.structuredContent.topics[0].partitions")
                .withFailMessage(() -> "describe_consumer_group did not report committed offset, end offset and lag per partition: "
                        + describeResp.body())
                .isEqualTo(json("""
                        [
                          {"partition": 0, "committedOffset": 40, "endOffset": 100, "lag": 60},
                          {"partition": 1, "committedOffset": 25, "endOffset": 70, "lag": 45}
                        ]"""));
        assertThatJson(describeResp.body()).inPath("result.structuredContent.totalLag")
                .withFailMessage(() -> "describe_consumer_group did not sum the lag of its partitions: " + describeResp.body())
                .isEqualTo(105);
    }

    @Test
    void carriesTheSameAnswerAsTextAndStructuredContent() throws Exception {
        // given
        groupReading("orders-consumers", "Stable",
                new Member("member-1", "client-1", "/10.0.0.1", new PartitionId("orders-v2", 0)));
        commit("orders-consumers", new PartitionId("orders-v2", 0), 40);
        end(new PartitionId("orders-v2", 0), 100);
        startServer();

        // when
        var describeResp = callToolWithArgs(TOOL, "{\"groupId\": \"orders-consumers\"}");

        // then
        assertThat(describeResp).hasStatusCode(200);
        assertThatJson(describeResp.body()).inPath("result.content[0].text").isString()
                .withFailMessage(() -> "the text content does not carry the same group as the structured content: " + describeResp.body())
                .satisfies(text -> assertThatJson(text).isEqualTo(json(structuredContent(describeResp.body()))));
    }

    @Test
    void describesAShareGroupWithLagNotApplicable() throws Exception {
        // given
        brokerAdmin.groups.put("orders-share", new GroupDescription("orders-share", "SHARE", "Stable", List.of()));
        startServer();

        // when
        var describeResp = callToolWithArgs(TOOL, "{\"groupId\": \"orders-share\"}");

        // then
        assertThat(describeResp).hasStatusCode(200);
        assertThatJson(describeResp.body()).inPath("result.isError")
                .withFailMessage(() -> "describe_consumer_group of a share group was a tool error: " + describeResp.body())
                .isEqualTo(false);
        assertThatJson(describeResp.body()).inPath("result.structuredContent")
                .withFailMessage(() -> "describe_consumer_group did not describe the share group with its type, state and no lag: "
                        + describeResp.body())
                .isEqualTo(json("""
                        {"groupId": "orders-share", "type": "SHARE", "state": "Stable", "lagApplicable": false}"""));
    }

    @Test
    void describesAPartitionWithoutACommitWithoutCommittedOffsetOrLag() throws Exception {
        // given: partition 0 is committed and 60 behind, partition 1 has never been committed
        groupReading("orders-consumers", "Stable",
                new Member("member-1", "client-1", "/10.0.0.1", new PartitionId("orders-v2", 0), new PartitionId("orders-v2", 1)));
        commit("orders-consumers", new PartitionId("orders-v2", 0), 40);
        end(new PartitionId("orders-v2", 0), 100);
        end(new PartitionId("orders-v2", 1), 70);
        startServer();

        // when
        var describeResp = callToolWithArgs(TOOL, "{\"groupId\": \"orders-consumers\"}");

        // then
        assertThat(describeResp).hasStatusCode(200);
        assertThatJson(describeResp.body()).inPath("result.isError")
                .withFailMessage(() -> "describe_consumer_group was a tool error: " + describeResp.body()).isEqualTo(false);
        assertThatJson(describeResp.body()).inPath("result.structuredContent.topics[0].partitions[1]")
                .withFailMessage(() -> "describe_consumer_group did not report only the end offset for the uncommitted partition: "
                        + describeResp.body())
                .isEqualTo(json("""
                        {"partition": 1, "endOffset": 70}"""));
        assertThatJson(describeResp.body()).inPath("result.structuredContent.uncommittedPartitions")
                .withFailMessage(() -> "describe_consumer_group did not count the uncommitted partition: " + describeResp.body())
                .isEqualTo(1);
        assertThatJson(describeResp.body()).inPath("result.structuredContent.totalLag")
                .withFailMessage(() -> "describe_consumer_group did not leave the uncommitted partition out of the total lag: "
                        + describeResp.body())
                .isEqualTo(60);
    }

    @Test
    void answersAToolErrorNamingAGroupThatDoesNotExist() throws Exception {
        // given
        startServer();

        // when
        var describeResp = callToolWithArgs(TOOL, "{\"groupId\": \"no-such-group\"}");

        // then
        assertThat(describeResp).hasStatusCode(200);
        assertThatJson(describeResp.body()).inPath("result.isError")
                .withFailMessage(() -> "describe_consumer_group of an unknown group was not a tool error: " + describeResp.body())
                .isEqualTo(true);
        assertThatJson(describeResp.body()).inPath("result.content[0].text")
                .withFailMessage(() -> "describe_consumer_group did not name 'no-such-group' in its error: " + describeResp.body())
                .isEqualTo("consumer group 'no-such-group' not found");
    }

    @Test
    void reportsLagOnThePhysicalTopicWithTheVirtualTopicsOnIt() throws Exception {
        // given: the virtual topic 'orders' points at 'orders-v2'; the group also reads 'payments-v1', which has none
        virtualTopics = new VirtualTopicManager(Map.of("orders", new VirtualTopicConfig("orders-v2")));
        groupReading("orders-consumers", "Stable",
                new Member("member-1", "client-1", "/10.0.0.1", new PartitionId("orders-v2", 0), new PartitionId("payments-v1", 0)));
        commit("orders-consumers", new PartitionId("orders-v2", 0), 40);
        commit("orders-consumers", new PartitionId("payments-v1", 0), 10);
        end(new PartitionId("orders-v2", 0), 100);
        end(new PartitionId("payments-v1", 0), 20);
        startServer();

        // when
        var describeResp = callToolWithArgs(TOOL, "{\"groupId\": \"orders-consumers\"}");

        // then
        assertThat(describeResp).hasStatusCode(200);
        assertThatJson(describeResp.body()).inPath("result.isError")
                .withFailMessage(() -> "describe_consumer_group was a tool error: " + describeResp.body()).isEqualTo(false);
        assertThatJson(describeResp.body()).inPath("result.structuredContent.topics[?(@.physicalTopic=='orders-v2')].virtualTopics")
                .withFailMessage(() -> "describe_consumer_group did not list the virtual topic 'orders' on 'orders-v2': " + describeResp.body())
                .isEqualTo(json("[[\"orders\"]]"));
        assertThatJson(describeResp.body()).inPath("result.structuredContent.topics[?(@.physicalTopic=='payments-v1')].virtualTopics")
                .withFailMessage(() -> "describe_consumer_group listed a virtual topic on 'payments-v1', which has none: " + describeResp.body())
                .isEqualTo(json("[[]]"));
    }

    @Test
    void marksLagAsUpperBoundWhenAVirtualTopicOnThePhysicalTopicFilters() throws Exception {
        // given: 'orders-acme' filters the physical topic 'orders-acme-v2', 'orders' does not filter 'orders-v2'
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2"),
                "orders-acme", new VirtualTopicConfig("orders-acme-v2", new HeaderEqualsFilterConfig("tenant", "acme"))));
        groupReading("orders-consumers", "Stable",
                new Member("member-1", "client-1", "/10.0.0.1", new PartitionId("orders-acme-v2", 0), new PartitionId("orders-v2", 0)));
        commit("orders-consumers", new PartitionId("orders-acme-v2", 0), 40);
        commit("orders-consumers", new PartitionId("orders-v2", 0), 40);
        end(new PartitionId("orders-acme-v2", 0), 100);
        end(new PartitionId("orders-v2", 0), 100);
        startServer();

        // when
        var describeResp = callToolWithArgs(TOOL, "{\"groupId\": \"orders-consumers\"}");

        // then
        assertThat(describeResp).hasStatusCode(200);
        assertThatJson(describeResp.body())
                .inPath("result.structuredContent.topics[?(@.physicalTopic=='orders-acme-v2')].lagIsUpperBound")
                .withFailMessage(() -> "lag through a filtered virtual topic was not marked as an upper bound: " + describeResp.body())
                .isEqualTo(json("[true]"));
        assertThatJson(describeResp.body())
                .inPath("result.structuredContent.topics[?(@.physicalTopic=='orders-v2')].lagIsUpperBound")
                .withFailMessage(() -> "lag through an unfiltered virtual topic was marked as an upper bound: " + describeResp.body())
                .isEqualTo(json("[false]"));
    }

    /// Adds a group in `state` with the given members.
    private void groupReading(String groupId, String state, Member... members) {
        List<GroupMember> groupMembers = List.of(members).stream()
                .map(member -> new GroupMember(member.id(), member.clientId(), member.host(), List.of(member.assigned())))
                .toList();
        brokerAdmin.groups.put(groupId, new GroupDescription(groupId, "CLASSIC", state, groupMembers));
    }

    private void commit(String groupId, PartitionId partition, long offset) {
        brokerAdmin.committed.computeIfAbsent(groupId, _ -> new HashMap<>()).put(partition, offset);
    }

    private void end(PartitionId partition, long offset) {
        brokerAdmin.latest.put(partition, offset);
    }

    /// A group member in a test set-up, assigned `assigned`.
    private record Member(String id, String clientId, String host, PartitionId... assigned) {
    }
}
