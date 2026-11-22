package io.jonasg.kawa.mcp.describetopic;

import io.jonasg.kawa.core.cluster.TopicMetadata;
import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.mcp.McpResource;
import io.modelcontextprotocol.json.McpJsonMapper;

import java.util.List;

/// The `kafka://topics/{topicName}` resource template: one topic on the broker, by its physical name, as the
/// [TopicDescriptionView] the `describe_topic` tool answers with. A virtual topic's name is not a resource, and
/// neither is a name the broker does not know: both are resource-not-found errors.
public final class TopicMcpResource extends McpResource {

    public static final String URI_TEMPLATE = "kafka://topics/{topicName}";

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

    /// The physical topics whose name starts with `prefix`, the same topics the resource can describe.
    @Override
    protected List<String> completions(String prefix) {
        return topicService.listTopics().physical().stream()
                .map(TopicMetadata::name)
                .filter(name -> name.startsWith(prefix))
                .sorted()
                .toList();
    }
}
