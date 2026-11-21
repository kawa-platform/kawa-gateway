package io.jonasg.kawa.mcp.listtopics;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.mcp.FilterView;
import io.jonasg.kawa.mcp.VirtualTopicViews;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.regex.Pattern;

/// The `list_topics` result: the topics the gateway serves, shaped for an agent rather than the
/// admin UI.
///
/// @param virtualTopics  the virtual topics, each mapped to a physical topic
/// @param physicalTopics the topics on the broker
public record ListTopicsView(List<VirtualTopicView> virtualTopics, List<PhysicalTopicView> physicalTopics) {

    /// The JSON Schema of a [ListTopicsView], published as the tool's `outputSchema`.
    static final String SCHEMA = """
            {
              "type": "object",
              "properties": {
                "virtualTopics": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "name": {
                        "type": "string"
                      },
                      "physicalTopic": {
                        "type": "string"
                      },
                      "partitions": {
                        "type": "integer"
                      },
                      "replicationFactor": {
                        "type": "integer"
                      },
                      "filter": {
                        "type": "object",
                        "properties": {
                          "type": {
                            "type": "string"
                          },
                          "description": {
                            "type": "string"
                          }
                        },
                        "required": [
                          "type",
                          "description"
                        ],
                        "additionalProperties": false
                      },
                      "valueFormat": {
                        "type": "string",
                        "enum": [
                          "json",
                          "avro",
                          "raw"
                        ]
                      },
                      "exposesPhysicalTopic": {
                        "type": "boolean"
                      }
                    },
                    "required": [
                      "name",
                      "physicalTopic",
                      "partitions",
                      "replicationFactor",
                      "valueFormat",
                      "exposesPhysicalTopic"
                    ],
                    "additionalProperties": false
                  }
                },
                "physicalTopics": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "name": {
                        "type": "string"
                      },
                      "partitions": {
                        "type": "integer"
                      },
                      "replicationFactor": {
                        "type": "integer"
                      }
                    },
                    "required": [
                      "name",
                      "partitions",
                      "replicationFactor"
                    ],
                    "additionalProperties": false
                  }
                }
              },
              "required": [
                "virtualTopics",
                "physicalTopics"
              ],
              "additionalProperties": false
            }
            """;

    /// A virtual topic.
    ///
    /// @param name                 the name clients use
    /// @param physicalTopic        the broker topic it maps to
    /// @param partitions           the physical topic's partition count, `0` when the broker does not know it
    /// @param replicationFactor    the physical topic's replica count per partition, `0` when unknown
    /// @param filter               the consume filter; absent when every record is served
    /// @param valueFormat          how record values are encoded: `json`, `avro`, or `raw` when not decoded
    /// @param exposesPhysicalTopic whether the physical topic is also served under its own name
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record VirtualTopicView(
            String name,
            String physicalTopic,
            int partitions,
            int replicationFactor,
            @Nullable FilterView filter,
            String valueFormat,
            boolean exposesPhysicalTopic
    ) {
    }

    /// A topic on the broker.
    ///
    /// @param name              the topic's name
    /// @param partitions        its partition count
    /// @param replicationFactor its replica count per partition
    record PhysicalTopicView(String name, int partitions, int replicationFactor) {
    }

    /// The topics `topicService` knows, restricted to the layers asked for and to the names `pattern` matches
    /// (a virtual topic by its virtual name, a physical topic by its physical name). A layer that is not asked for
    /// is an empty list.
    ///
    /// @param pattern         searched anywhere in the name; `null` lists every topic
    /// @param includeVirtual  whether to list the virtual topics
    /// @param includePhysical whether to list the physical topics
    static ListTopicsView of(TopicService topicService, @Nullable Pattern pattern, boolean includeVirtual, boolean includePhysical) {
        var listing = topicService.listTopics();
        var virtualTopics = includeVirtual
                ? listing.virtual().stream()
                        .filter(entry -> matches(pattern, entry.name()))
                        .map(entry -> new VirtualTopicView(
                                entry.name(),
                                entry.physicalTopic(),
                                topicService.partitionCount(entry.physicalTopic()),
                                topicService.replicationFactor(entry.physicalTopic()),
                                VirtualTopicViews.filter(entry.filter()),
                                VirtualTopicViews.valueFormat(entry.valueFormat()),
                                entry.exposePhysicalTopic()))
                        .toList()
                : List.<VirtualTopicView>of();
        var physicalTopics = includePhysical
                ? listing.physical().stream()
                        .filter(topic -> matches(pattern, topic.name()))
                        .map(topic -> new PhysicalTopicView(
                                topic.name(),
                                topicService.partitionCount(topic.name()),
                                topicService.replicationFactor(topic.name())))
                        .toList()
                : List.<PhysicalTopicView>of();
        return new ListTopicsView(virtualTopics, physicalTopics);
    }

    private static boolean matches(@Nullable Pattern pattern, String name) {
        return pattern == null || pattern.matcher(name).find();
    }
}
