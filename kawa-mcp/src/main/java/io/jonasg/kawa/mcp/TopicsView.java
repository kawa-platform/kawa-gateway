package io.jonasg.kawa.mcp;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;

/// The `list_topics` result: the topics the gateway serves, shaped for an agent rather than the
/// admin UI.
///
/// @param virtualTopics  the virtual topics, each mapped to a physical topic
/// @param physicalTopics the topics on the broker
record TopicsView(List<VirtualTopicView> virtualTopics, List<PhysicalTopicView> physicalTopics) {

    /// The JSON Schema of a [TopicsView], published as the tool's `outputSchema`.
    static final Map<String, Object> SCHEMA = Map.of(
            "type", "object",
            "properties", Map.of(
                    "virtualTopics", Map.of("type", "array", "items", VirtualTopicView.SCHEMA),
                    "physicalTopics", Map.of("type", "array", "items", PhysicalTopicView.SCHEMA)),
            "required", List.of("virtualTopics", "physicalTopics"),
            "additionalProperties", false);

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

        static final Map<String, Object> SCHEMA = Map.of(
                "type", "object",
                "properties", Map.of(
                        "name", Map.of("type", "string"),
                        "physicalTopic", Map.of("type", "string"),
                        "partitions", Map.of("type", "integer"),
                        "replicationFactor", Map.of("type", "integer"),
                        "filter", FilterView.SCHEMA,
                        "valueFormat", Map.of("type", "string", "enum", List.of("json", "avro", "raw")),
                        "exposesPhysicalTopic", Map.of("type", "boolean")),
                "required", List.of("name", "physicalTopic", "partitions", "replicationFactor", "valueFormat",
                        "exposesPhysicalTopic"),
                "additionalProperties", false);
    }

    /// A topic on the broker.
    ///
    /// @param name              the topic's name
    /// @param partitions        its partition count
    /// @param replicationFactor its replica count per partition
    record PhysicalTopicView(String name, int partitions, int replicationFactor) {

        static final Map<String, Object> SCHEMA = Map.of(
                "type", "object",
                "properties", Map.of(
                        "name", Map.of("type", "string"),
                        "partitions", Map.of("type", "integer"),
                        "replicationFactor", Map.of("type", "integer")),
                "required", List.of("name", "partitions", "replicationFactor"),
                "additionalProperties", false);
    }

    /// A virtual topic's consume filter: only records it matches are served.
    ///
    /// @param type        the filter kind, as in the config: `headerEquals`, `headerContains`,
    ///                    `headerStartsWith`, `headerMatches` or `cel`
    /// @param description what it matches, e.g. `header 'region' equals 'eu'` or the CEL expression
    record FilterView(String type, String description) {

        static final Map<String, Object> SCHEMA = Map.of(
                "type", "object",
                "properties", Map.of(
                        "type", Map.of("type", "string"),
                        "description", Map.of("type", "string")),
                "required", List.of("type", "description"),
                "additionalProperties", false);
    }
}
