package io.jonasg.kawa.mcp;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.jonasg.kawa.http.consumergroups.ConsumerGroupService.PartitionOffsets;
import io.jonasg.kawa.mcp.TopicsView.FilterView;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;

/// The `describe_consumer_group` result: a consumer group, its members, and the lag of every
/// partition it reads, grouped by physical topic. Offsets belong to the physical topic, so lag is
/// reported against it, with the virtual topics that read it.
///
/// @param groupId         the group's id
/// @param state           the group's state as the broker names it, e.g. `Stable`, `Empty` or `PreparingRebalance`
/// @param totalLag        the summed lag of every partition with a known lag
/// @param lagIsUpperBound whether any of [#topics] has an upper-bound lag
/// @param members         the group's current members
/// @param topics          the physical topics the group reads, sorted by name
record ConsumerGroupView(
        String groupId,
        String state,
        long totalLag,
        boolean lagIsUpperBound,
        List<MemberView> members,
        List<TopicLagView> topics
) {

    static final String TOTAL_LAG = "Sum of end offset minus committed offset over the partitions with a committed offset.";
    static final String UPPER_BOUND = "True when a virtual topic reading the physical topic filters records: records the "
            + "filter drops still count as lag, but the consumer never receives them, so the real backlog may be smaller.";

    /// The JSON Schema of a [ConsumerGroupView], published as the tool's `outputSchema`.
    static final Map<String, Object> SCHEMA = Map.of(
            "type", "object",
            "properties", Map.of(
                    "groupId", Map.of("type", "string"),
                    "state", Map.of("type", "string"),
                    "totalLag", Map.of("type", "integer", "description", TOTAL_LAG),
                    "lagIsUpperBound", Map.of("type", "boolean", "description", UPPER_BOUND),
                    "members", Map.of("type", "array", "items", MemberView.SCHEMA),
                    "topics", Map.of("type", "array", "items", TopicLagView.SCHEMA)),
            "required", List.of("groupId", "state", "totalLag", "lagIsUpperBound", "members", "topics"),
            "additionalProperties", false);

    /// A member of the group.
    ///
    /// @param memberId   the id the coordinator gave the member
    /// @param clientId   the client id the consumer sent
    /// @param host       the host the consumer connected from, as the broker saw it
    /// @param assignment the physical partitions assigned to it, by topic
    record MemberView(String memberId, String clientId, String host, List<AssignmentView> assignment) {

        static final Map<String, Object> SCHEMA = Map.of(
                "type", "object",
                "properties", Map.of(
                        "memberId", Map.of("type", "string"),
                        "clientId", Map.of("type", "string"),
                        "host", Map.of("type", "string"),
                        "assignment", Map.of("type", "array", "items", AssignmentView.SCHEMA)),
                "required", List.of("memberId", "clientId", "host", "assignment"),
                "additionalProperties", false);
    }

    /// The partitions of one physical topic assigned to a member.
    ///
    /// @param topic      the physical topic
    /// @param partitions the partition numbers, ascending
    record AssignmentView(String topic, List<Integer> partitions) {

        static final Map<String, Object> SCHEMA = Map.of(
                "type", "object",
                "properties", Map.of(
                        "topic", Map.of("type", "string"),
                        "partitions", Map.of("type", "array", "items", Map.of("type", "integer"))),
                "required", List.of("topic", "partitions"),
                "additionalProperties", false);
    }

    /// The group's lag on one physical topic.
    ///
    /// @param physicalTopic   the broker topic the offsets belong to
    /// @param virtualTopics   the virtual topics backed by it, which the group's consumers may use instead of its name
    /// @param totalLag        the summed lag of its partitions with a known lag
    /// @param lagIsUpperBound whether one of [#virtualTopics] filters records
    /// @param partitions      its partitions the group is assigned or has committed, ascending
    record TopicLagView(
            String physicalTopic,
            List<VirtualTopicReaderView> virtualTopics,
            long totalLag,
            boolean lagIsUpperBound,
            List<PartitionLagView> partitions
    ) {

        static final Map<String, Object> SCHEMA = Map.of(
                "type", "object",
                "properties", Map.of(
                        "physicalTopic", Map.of("type", "string", "description",
                                "The broker topic the offsets and lag belong to; virtual topics have no offsets of their own."),
                        "virtualTopics", Map.of("type", "array", "items", VirtualTopicReaderView.SCHEMA, "description",
                                "The gateway's virtual topics backed by this physical topic."),
                        "totalLag", Map.of("type", "integer", "description", TOTAL_LAG),
                        "lagIsUpperBound", Map.of("type", "boolean", "description", UPPER_BOUND),
                        "partitions", Map.of("type", "array", "items", PartitionLagView.SCHEMA)),
                "required", List.of("physicalTopic", "virtualTopics", "totalLag", "lagIsUpperBound", "partitions"),
                "additionalProperties", false);
    }

    /// A virtual topic backed by a physical topic the group reads.
    ///
    /// @param name   the virtual topic's name
    /// @param filter its consume filter; absent when every record is served
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record VirtualTopicReaderView(String name, @Nullable FilterView filter) {

        static final Map<String, Object> SCHEMA = Map.of(
                "type", "object",
                "properties", Map.of(
                        "name", Map.of("type", "string"),
                        "filter", FilterView.SCHEMA),
                "required", List.of("name"),
                "additionalProperties", false);
    }

    /// The group's position on one partition.
    ///
    /// @param partition       the partition number
    /// @param committedOffset the group's committed offset; absent when it has committed none
    /// @param endOffset       the partition's end offset; absent when the broker does not know the partition
    /// @param lag             end offset minus committed offset; absent when either is
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record PartitionLagView(int partition, @Nullable Long committedOffset, @Nullable Long endOffset, @Nullable Long lag) {

        static final Map<String, Object> SCHEMA = Map.of(
                "type", "object",
                "properties", Map.of(
                        "partition", Map.of("type", "integer"),
                        "committedOffset", Map.of("type", "integer", "description",
                                "Absent when the group has committed no offset for this partition (not the same as 0)."),
                        "endOffset", Map.of("type", "integer"),
                        "lag", Map.of("type", "integer", "description",
                                "End offset minus committed offset; absent when there is no committed offset.")),
                "required", List.of("partition"),
                "additionalProperties", false);

        static PartitionLagView of(PartitionOffsets offsets) {
            return new PartitionLagView(offsets.partition(), offsets.committedOffset(), offsets.endOffset(), lag(offsets));
        }

        /// End offset minus committed offset, never negative, or `null` when either offset is unknown.
        static @Nullable Long lag(PartitionOffsets offsets) {
            Long committed = offsets.committedOffset();
            Long end = offsets.endOffset();
            return committed == null || end == null ? null : Math.max(0, end - committed);
        }
    }
}
