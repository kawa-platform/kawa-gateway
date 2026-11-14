package io.jonasg.kawa.mcp;

import io.jonasg.kawa.http.consumergroups.ConsumerGroupService;
import io.jonasg.kawa.http.consumergroups.ConsumerGroupService.ConsumerGroupOffsets;
import io.jonasg.kawa.http.consumergroups.ConsumerGroupService.PartitionOffsets;
import io.jonasg.kawa.http.topics.TopicAdmin.GroupMember;
import io.jonasg.kawa.http.topics.TopicAdmin.GroupPartition;
import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.mcp.ConsumerGroupView.AssignmentView;
import io.jonasg.kawa.mcp.ConsumerGroupView.MemberView;
import io.jonasg.kawa.mcp.ConsumerGroupView.PartitionLagView;
import io.jonasg.kawa.mcp.ConsumerGroupView.TopicLagView;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpStatelessServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

/// The `describe_consumer_group` tool: one consumer group's state, members and per-partition
/// committed offset, end offset and lag, read from the [ConsumerGroupService] and answered as a
/// [ConsumerGroupView], both as structured content and as its JSON text. Lag is grouped by
/// physical topic, with the virtual topics backed by it (see [VirtualTopicReaders]). An unknown
/// group is a tool error naming it.
final class DescribeConsumerGroupTool {

    static final String NAME = "describe_consumer_group";

    private static final String GROUP_ID = "groupId";

    private static final Map<String, Object> INPUT_SCHEMA = Map.of(
            "type", "object",
            "properties", Map.of(
                    GROUP_ID, Map.of("type", "string", "description", "The id of the consumer group to describe.")),
            "required", List.of(GROUP_ID));

    private final ConsumerGroupService consumerGroups;
    private final TopicService topics;
    private final McpJsonMapper mapper;

    DescribeConsumerGroupTool(ConsumerGroupService consumerGroups, TopicService topics, McpJsonMapper mapper) {
        this.consumerGroups = consumerGroups;
        this.topics = topics;
        this.mapper = mapper;
    }

    SyncToolSpecification specification() {
        var tool = Tool.builder(NAME, INPUT_SCHEMA)
                .description("Describes one consumer group from live broker data: its state, its members and their "
                        + "assigned partitions, and for every partition it reads the committed offset, the end offset "
                        + "and the lag (end offset minus committed offset). A partition without a committed offset has "
                        + "no committedOffset and no lag. Offsets belong to physical topics, so lag is reported per "
                        + "physicalTopic together with the virtualTopics backed by it; when one of those filters "
                        + "records, lagIsUpperBound is true because filtered-out records count as lag but are never "
                        + "delivered.")
                .outputSchema(ConsumerGroupView.SCHEMA)
                .annotations(ToolResults.READ_ONLY)
                .build();
        return SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((context, request) -> ToolResults.guarded(NAME, context, () -> call(request)))
                .build();
    }

    private CallToolResult call(CallToolRequest request) {
        var group = consumerGroups.describe(groupId(request));
        return ToolResults.success(mapper, groupView(group, VirtualTopicReaders.of(topics)));
    }

    private static String groupId(CallToolRequest request) {
        var arguments = request.arguments();
        var argument = arguments == null ? null : arguments.get(GROUP_ID);
        if (!(argument instanceof String groupId) || groupId.isBlank()) {
            throw new IllegalArgumentException("groupId must be a non-blank string");
        }
        return groupId;
    }

    private static ConsumerGroupView groupView(ConsumerGroupOffsets group, VirtualTopicReaders readers) {
        Map<String, List<PartitionOffsets>> byTopic = group.partitions().stream()
                .collect(Collectors.groupingBy(PartitionOffsets::topic, TreeMap::new, Collectors.toList()));
        List<TopicLagView> topicViews = byTopic.entrySet().stream()
                .map(entry -> {
                    var partitions = entry.getValue().stream().map(PartitionLagView::of).toList();
                    return new TopicLagView(entry.getKey(), readers.readersOf(entry.getKey()), totalLag(partitions),
                            readers.filters(entry.getKey()), partitions);
                })
                .toList();
        long totalLag = topicViews.stream().mapToLong(TopicLagView::totalLag).sum();
        boolean upperBound = topicViews.stream().anyMatch(TopicLagView::lagIsUpperBound);
        var members = group.members().stream().map(DescribeConsumerGroupTool::memberView).toList();
        return new ConsumerGroupView(group.groupId(), group.state(), totalLag, upperBound, members, topicViews);
    }

    private static long totalLag(List<PartitionLagView> partitions) {
        return partitions.stream().map(PartitionLagView::lag).filter(Objects::nonNull).mapToLong(Long::longValue).sum();
    }

    private static MemberView memberView(GroupMember member) {
        var assignment = member.assignment().stream()
                .collect(Collectors.groupingBy(GroupPartition::topic, TreeMap::new,
                        Collectors.mapping(GroupPartition::partition, Collectors.toList())))
                .entrySet().stream()
                .map(entry -> new AssignmentView(entry.getKey(), entry.getValue().stream().sorted().toList()))
                .toList();
        return new MemberView(member.memberId(), member.clientId(), member.host(), assignment);
    }
}
