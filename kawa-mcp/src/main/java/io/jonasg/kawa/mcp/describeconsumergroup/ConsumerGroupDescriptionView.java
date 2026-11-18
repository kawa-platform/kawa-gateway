package io.jonasg.kawa.mcp.describeconsumergroup;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.jspecify.annotations.Nullable;

import java.util.List;

/// The `describe_consumer_group` result: one group with its members and the lag of each partition it reads, shaped
/// for an agent. When lag does not apply, only the group's id, type and state are given.
///
/// @param groupId               the group id
/// @param type                  the group type as the broker reports it, e.g. `CLASSIC`, `CONSUMER`, `SHARE` or `STREAMS`
/// @param state                 the group state as the broker reports it, e.g. `Stable` or `Empty`
/// @param lagApplicable         whether the group commits offsets per partition, so that it has lag
/// @param members               the members, in the order the broker reports them; absent when lag does not apply
/// @param topics                the physical topics the group reads; absent when lag does not apply
/// @param totalLag              end offset minus committed offset, summed over the partitions the group committed;
///                              absent when lag does not apply
/// @param uncommittedPartitions the partitions the group reads without a commit, not counted in `totalLag`; absent
///                              when lag does not apply
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ConsumerGroupDescriptionView(
        String groupId,
        String type,
        String state,
        boolean lagApplicable,
        @Nullable List<MemberView> members,
        @Nullable List<TopicLagView> topics,
        @Nullable Long totalLag,
        @Nullable Integer uncommittedPartitions
) {

    /// The JSON Schema of a [ConsumerGroupDescriptionView], published as the tool's `outputSchema`.
    static final String SCHEMA = """
            {
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
                "lagApplicable": {
                  "type": "boolean"
                },
                "members": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "memberId": {
                        "type": "string"
                      },
                      "clientId": {
                        "type": "string"
                      },
                      "host": {
                        "type": "string"
                      },
                      "assignedPartitions": {
                        "type": "array",
                        "items": {
                          "type": "object",
                          "properties": {
                            "topic": {
                              "type": "string"
                            },
                            "partition": {
                              "type": "integer"
                            }
                          },
                          "required": [
                            "topic",
                            "partition"
                          ],
                          "additionalProperties": false
                        }
                      }
                    },
                    "required": [
                      "memberId",
                      "clientId",
                      "host",
                      "assignedPartitions"
                    ],
                    "additionalProperties": false
                  }
                },
                "topics": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "physicalTopic": {
                        "type": "string"
                      },
                      "virtualTopics": {
                        "type": "array",
                        "items": {
                          "type": "string"
                        }
                      },
                      "lagIsUpperBound": {
                        "type": "boolean"
                      },
                      "partitions": {
                        "type": "array",
                        "items": {
                          "type": "object",
                          "properties": {
                            "partition": {
                              "type": "integer"
                            },
                            "committedOffset": {
                              "type": "integer"
                            },
                            "endOffset": {
                              "type": "integer"
                            },
                            "lag": {
                              "type": "integer"
                            }
                          },
                          "required": [
                            "partition"
                          ],
                          "additionalProperties": false
                        }
                      }
                    },
                    "required": [
                      "physicalTopic",
                      "virtualTopics",
                      "lagIsUpperBound",
                      "partitions"
                    ],
                    "additionalProperties": false
                  }
                },
                "totalLag": {
                  "type": "integer"
                },
                "uncommittedPartitions": {
                  "type": "integer"
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
            """;

    /// A member of the group.
    ///
    /// @param memberId           the member id the broker gave it
    /// @param clientId           the client id the consumer sent
    /// @param host               the host the consumer connected from
    /// @param assignedPartitions the partitions assigned to it, on their physical topics
    record MemberView(String memberId, String clientId, String host, List<AssignedPartitionView> assignedPartitions) {
    }

    /// A partition assigned to a member.
    ///
    /// @param topic     the physical topic
    /// @param partition the partition number
    record AssignedPartitionView(String topic, int partition) {
    }

    /// The lag of one physical topic the group reads.
    ///
    /// @param physicalTopic   the broker topic the offsets belong to
    /// @param virtualTopics   the virtual topics on it, empty when none
    /// @param lagIsUpperBound whether one of those virtual topics filters: records the filter drops count as lag
    ///                        but are never delivered
    /// @param partitions      the partitions the group reads, in ascending order
    record TopicLagView(String physicalTopic, List<String> virtualTopics, boolean lagIsUpperBound, List<PartitionLagView> partitions) {
    }

    /// The offsets of one partition the group reads.
    ///
    /// @param partition       the partition number
    /// @param committedOffset the offset the group committed; absent when it has none
    /// @param endOffset       the partition's end offset; absent when the broker does not know the partition
    /// @param lag             end minus committed; absent unless both are known
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record PartitionLagView(int partition, @Nullable Long committedOffset, @Nullable Long endOffset, @Nullable Long lag) {
    }
}
