package io.jonasg.kawa.mcp;

import java.util.List;
import java.util.Map;

/// The `list_consumer_groups` result: every consumer group on the broker with its total lag.
///
/// @param groups the groups, sorted by id
record ConsumerGroupsView(List<GroupView> groups) {

    /// The JSON Schema of a [ConsumerGroupsView], published as the tool's `outputSchema`.
    static final Map<String, Object> SCHEMA = Map.of(
            "type", "object",
            "properties", Map.of("groups", Map.of("type", "array", "items", GroupView.SCHEMA)),
            "required", List.of("groups"),
            "additionalProperties", false);

    /// A consumer group.
    ///
    /// @param groupId         the group's id
    /// @param state           the group's state as the broker names it, e.g. `Stable`, `Empty` or `PreparingRebalance`
    /// @param totalLag        the summed lag of the partitions the group has committed
    /// @param lagIsUpperBound whether a virtual topic backed by one of those partitions' topics filters records
    record GroupView(String groupId, String state, long totalLag, boolean lagIsUpperBound) {

        static final Map<String, Object> SCHEMA = Map.of(
                "type", "object",
                "properties", Map.of(
                        "groupId", Map.of("type", "string"),
                        "state", Map.of("type", "string"),
                        "totalLag", Map.of("type", "integer", "description", ConsumerGroupView.TOTAL_LAG),
                        "lagIsUpperBound", Map.of("type", "boolean", "description", ConsumerGroupView.UPPER_BOUND)),
                "required", List.of("groupId", "state", "totalLag", "lagIsUpperBound"),
                "additionalProperties", false);
    }
}
