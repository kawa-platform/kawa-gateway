package io.jonasg.kawa.mcp.describetopic;

import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.mcp.McpResource;
import io.modelcontextprotocol.json.McpJsonMapper;

/// The `kawa://topics/{topicName}` resource template: one topic on the broker, by its physical name, as the
/// [TopicDescriptionView] the `describe_topic` tool answers with. A virtual topic's name is not a resource, and
/// neither is a name the broker does not know: both are resource-not-found errors.
public final class TopicMcpResource extends McpResource {

    public static final String URI_TEMPLATE = "kawa://topics/{topicName}";

    private final TopicService topicService;

    public TopicMcpResource(McpJsonMapper mapper, TopicService topicService) {
        super(mapper, URI_TEMPLATE, "topic",
                "One topic on the broker by its name, with its partitions, replication factor and settings.");
        this.topicService = topicService;
    }

    @Override
    protected TopicDescriptionView read(String requestedUri) {
        return TopicDescriptionView.of(topicService.describePhysicalTopic(variable(requestedUri)));
    }
}
