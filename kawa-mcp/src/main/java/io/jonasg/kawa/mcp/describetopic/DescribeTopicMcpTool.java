package io.jonasg.kawa.mcp.describetopic;

import io.jonasg.kawa.http.topics.TopicNotFoundException;
import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.mcp.McpTool;
import io.jonasg.kawa.mcp.McpToolException;
import io.jonasg.kawa.mcp.VirtualTopicViews;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;

/// The `describe_topic` tool: one topic, read from the [TopicService] and answered as a
/// [TopicDescriptionView]. The `topicName` argument names a virtual topic or a topic on the broker; the view
/// reports the physical topic that holds its records.
public final class DescribeTopicMcpTool extends McpTool {

    private static final String NAME = "describe_topic";
    private static final String INPUT_SCHEMA = """
            {
              "type": "object",
              "properties": {
                "topicName": {
                  "type": "string",
                  "description": "Name of the Kafka topic"
                }
              },
              "required": ["topicName"]
            }""";

    private final TopicService topicService;

    public DescribeTopicMcpTool(McpJsonMapper mapper, TopicService topicService) {
        super(mapper);
        this.topicService = topicService;
    }

    @Override
    protected String name() {
        return NAME;
    }

    @Override
    protected String description() {
        return "Describes one topic by its name. A virtual topic is reported with the physical topic it maps to, "
                + "its consume filter, value format and expose setting. A physical topic is reported with its "
                + "partitions, replication factor and settings. Also reports when the name is a physical topic "
                + "that a virtual topic of the same name hides.";
    }

    @Override
    protected String inputSchema() {
        return INPUT_SCHEMA;
    }

    @Override
    protected String outputSchema() {
        return TopicDescriptionView.SCHEMA;
    }

    @Override
    protected TopicDescriptionView call(CallToolRequest request) {
        TopicService.TopicDescription description;
        String topicName = getRequiredArg(request, "topicName");
        try {
            description = topicService.describeTopic(topicName);
        } catch (TopicNotFoundException ex) {
            throw new McpToolException(ex.getMessage());
        }
        return view(description);
    }

    /// The view of a described topic: its virtual topic details when the name is virtual, and the physical topic
    /// that holds the records.
    private static TopicDescriptionView view(TopicService.TopicDescription description) {
        var virtual = description.virtualTopic();
        return new TopicDescriptionView(
                description.name(),
                virtual == null ? null : new TopicDescriptionView.VirtualTopicDetails(
                        virtual.name(),
                        VirtualTopicViews.filter(virtual.filter()),
                        VirtualTopicViews.valueFormat(virtual.valueFormat()),
                        virtual.exposePhysicalTopic()
                ),
                new TopicDescriptionView.PhysicalTopicDetails(
                        description.physicalTopic(),
                        description.partitions(),
                        description.replicationFactor(),
                        description.configs()
                ),
                description.shadowsPhysicalTopic()
        );
    }
}
