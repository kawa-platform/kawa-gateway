package io.jonasg.kawa.mcp;

import io.jonasg.kawa.http.consumergroups.ConsumerGroupService;
import io.jonasg.kawa.http.consumergroups.ConsumerGroupService.ConsumerGroupOffsets;
import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.mcp.ConsumerGroupView.PartitionLagView;
import io.jonasg.kawa.mcp.ConsumerGroupsView.GroupView;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpStatelessServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;

import java.util.Map;
import java.util.Objects;

/// The `list_consumer_groups` tool: every consumer group on the broker with its state and total lag,
/// read from the [ConsumerGroupService] in a fixed number of batched broker calls and answered as a
/// [ConsumerGroupsView], both as structured content and as its JSON text.
final class ListConsumerGroupsTool {

    static final String NAME = "list_consumer_groups";

    private static final Map<String, Object> INPUT_SCHEMA = Map.of("type", "object", "properties", Map.of());

    private final ConsumerGroupService consumerGroups;
    private final TopicService topics;
    private final McpJsonMapper mapper;

    ListConsumerGroupsTool(ConsumerGroupService consumerGroups, TopicService topics, McpJsonMapper mapper) {
        this.consumerGroups = consumerGroups;
        this.topics = topics;
        this.mapper = mapper;
    }

    SyncToolSpecification specification() {
        var tool = Tool.builder(NAME, INPUT_SCHEMA)
                .description("Lists the consumer groups on the Kafka cluster with their state (e.g. Stable, Empty, "
                        + "PreparingRebalance) and total lag, from live broker data. Lag is measured on the physical "
                        + "topics; it is an upper bound when a virtual topic backed by one of them filters records. "
                        + "Use describe_consumer_group for members and per-partition lag.")
                .outputSchema(ConsumerGroupsView.SCHEMA)
                .annotations(ToolResults.READ_ONLY)
                .build();
        return SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((context, _) -> ToolResults.guarded(NAME, context, this::call))
                .build();
    }

    private CallToolResult call() {
        var readers = VirtualTopicReaders.of(topics);
        var groups = consumerGroups.list().stream().map(group -> groupView(group, readers)).toList();
        return ToolResults.success(mapper, new ConsumerGroupsView(groups));
    }

    private static GroupView groupView(ConsumerGroupOffsets group, VirtualTopicReaders readers) {
        long totalLag = group.partitions().stream()
                .map(PartitionLagView::lag)
                .filter(Objects::nonNull)
                .mapToLong(Long::longValue)
                .sum();
        boolean upperBound = group.partitions().stream().anyMatch(partition -> readers.filters(partition.topic()));
        return new GroupView(group.groupId(), group.state(), totalLag, upperBound);
    }
}
