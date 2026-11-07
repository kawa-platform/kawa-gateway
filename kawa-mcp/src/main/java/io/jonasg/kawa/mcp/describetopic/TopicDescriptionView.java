package io.jonasg.kawa.mcp.describetopic;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.jonasg.kawa.mcp.FilterView;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/// The `describe_topic` result: one topic as clients see it, with the physical topic behind it.
///
/// @param topicName            the topic name that was asked for
/// @param virtualTopic         the virtual topic of that name; absent when `topicName` is only a physical topic
/// @param physicalTopic        the broker topic that holds the records: the virtual topic's target, or the
///                             topic itself
/// @param shadowsPhysicalTopic whether a physical topic with the same name as the virtual topic also exists
///                             on the broker but is hidden from clients by it
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TopicDescriptionView(
        String topicName,
        @Nullable VirtualTopicDetails virtualTopic,
        PhysicalTopicDetails physicalTopic,
        boolean shadowsPhysicalTopic
) {

    /// The JSON Schema of a [TopicDescriptionView], published as the tool's `outputSchema`.
    static final String SCHEMA = """
            {
              "type": "object",
              "properties": {
                "topicName": {
                  "type": "string"
                },
                "virtualTopic": {
                  "type": "object",
                  "properties": {
                    "topicName": {
                      "type": "string"
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
                    "topicName",
                    "valueFormat",
                    "exposesPhysicalTopic"
                  ],
                  "additionalProperties": false
                },
                "physicalTopic": {
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
                    },
                    "settings": {
                      "type": "object",
                      "additionalProperties": {
                        "type": "string"
                      }
                    }
                  },
                  "required": [
                    "name",
                    "partitions",
                    "replicationFactor",
                    "settings"
                  ],
                  "additionalProperties": false
                },
                "shadowsPhysicalTopic": {
                  "type": "boolean"
                }
              },
              "required": [
                "topicName",
                "physicalTopic",
                "shadowsPhysicalTopic"
              ],
              "additionalProperties": false
            }
            """;

    /// What makes a topic virtual.
    ///
    /// @param topicName            the virtual topic's name
    /// @param filter               the consume filter; absent when every record is served
    /// @param valueFormat          how record values are encoded: `json`, `avro`, or `raw` when not decoded
    /// @param exposesPhysicalTopic whether the physical topic is also served under its own name
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record VirtualTopicDetails(
            String topicName,
            @Nullable FilterView filter,
            String valueFormat,
            boolean exposesPhysicalTopic
    ) {
    }

    /// A topic on the broker.
    ///
    /// @param name              the topic's name
    /// @param partitions        its partition count, `0` when the broker does not know it
    /// @param replicationFactor its replica count per partition, `0` when unknown
    /// @param settings          the configs set on the topic itself, such as `retention.ms`; broker defaults
    ///                          are left out
    record PhysicalTopicDetails(
            String name,
            int partitions,
            int replicationFactor,
            Map<String, String> settings
    ) {
    }
}
