package io.jonasg.kawa.mcp.listconsumergroups;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.jspecify.annotations.Nullable;

import java.util.List;

/// The `list_consumer_groups` result: every group on the cluster, with its lag where lag applies, shaped for an agent.
///
/// @param groups the groups, empty when there are none
public record ListConsumerGroupsView(List<ConsumerGroupView> groups) {

    /// The JSON Schema of a [ListConsumerGroupsView], published as the tool's `outputSchema`.
    static final String SCHEMA = """
            {
              "type": "object",
              "properties": {
                "groups": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "groupId": {
                        "type": "string"
                      },
                      "type": {
                        "type": "string"
                      },
                      "state": {
                        "type": "string"
                      },
                      "protocolType": {
                        "type": "string"
                      },
                      "lagApplicable": {
                        "type": "boolean"
                      },
                      "totalLag": {
                        "type": "integer"
                      },
                      "uncommittedPartitions": {
                        "type": "integer"
                      },
                      "lagIsUpperBound": {
                        "type": "boolean"
                      }
                    },
                    "required": [
                      "groupId",
                      "type",
                      "state",
                      "lagApplicable"
                    ],
                    "additionalProperties": false
                  }
                }
              },
              "required": [
                "groups"
              ],
              "additionalProperties": false
            }
            """;

    /// A group. The lag fields are absent when lag does not apply.
    ///
    /// @param groupId               the group id
    /// @param type                  the group type as the broker reports it, e.g. `CLASSIC`, `CONSUMER`, `SHARE` or `STREAMS`
    /// @param state                 the group state as the broker reports it, e.g. `Stable` or `Empty`
    /// @param protocolType          the protocol type the group reports, e.g. `consumer` or `connect`; absent when none
    /// @param lagApplicable         whether the group commits offsets per partition, so that it has lag
    /// @param totalLag              end offset minus committed offset, summed over the partitions the group committed
    /// @param uncommittedPartitions the partitions the group reads without a commit, not counted in `totalLag`
    /// @param lagIsUpperBound       whether the group reads a physical topic a filtered virtual topic points at: records
    ///                              the filter drops count as lag but are never delivered
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record ConsumerGroupView(
            String groupId,
            String type,
            String state,
            @Nullable String protocolType,
            boolean lagApplicable,
            @Nullable Long totalLag,
            @Nullable Integer uncommittedPartitions,
            @Nullable Boolean lagIsUpperBound
    ) {
    }
}
