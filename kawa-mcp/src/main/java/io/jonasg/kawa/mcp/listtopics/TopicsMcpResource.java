package io.jonasg.kawa.mcp.listtopics;

import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.mcp.McpResource;
import io.modelcontextprotocol.json.McpJsonMapper;

/// The `kawa://topics` resource: every topic on the broker, as the physical part of the [ListTopicsView] the
/// `list_topics` tool answers with. Virtual topics are not listed; they are reached through the tools.
public final class TopicsMcpResource extends McpResource {

    public static final String URI = "kawa://topics";

    private final TopicService topicService;

    public TopicsMcpResource(McpJsonMapper mapper, TopicService topicService) {
        super(mapper, URI, "topics",
                "The topics on the broker, with partitions and replication factor. Virtual topics are not listed.");
        this.topicService = topicService;
    }

    @Override
    protected ListTopicsView read(String requestedUri) {
        return ListTopicsView.of(topicService, null, false, true);
    }
}
