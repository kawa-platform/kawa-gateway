package io.jonasg.kawa.mcp.listtopics;

import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.mcp.McpTool;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import org.jspecify.annotations.Nullable;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/// The `list_topics` tool: the gateway's virtual topics and the broker's physical topics, read from
/// the [TopicService] and answered as a [ListTopicsView], both as structured content and as its JSON text.
/// The optional `name` argument is a regular expression searched anywhere in the name; a virtual topic
/// matches by its virtual name, a physical topic by its physical name.
public final class ListTopicsMcpTool extends McpTool {

    private static final String NAME = "list_topics";

    static final int MAX_NAME_PATTERN_LENGTH = 256;

    private static final String VIRTUAL = "virtual";
    private static final String PHYSICAL = "physical";

    private static final String INPUT_SCHEMA = """
            {
              "type": "object",
              "properties": {
                "name": {
                  "type": "string",
                  "description": "Optional case-sensitive regular expression, at most %d characters, \
            searched anywhere in the name. Omit or leave blank to list every topic."
                },
                "type": {
                  "type": "string",
                  "description": "Optional layer to list: 'virtual' for the gateway's virtual topics, \
            'physical' for the broker's topics. Omit or leave blank to list both. \
            A layer that is not requested is returned as an empty list."
                }
              }
            }
            """.formatted(MAX_NAME_PATTERN_LENGTH);

    private final TopicService topicService;

    public ListTopicsMcpTool(McpJsonMapper mapper, TopicService topicService) {
        super(mapper);
        this.topicService = topicService;
    }

    @Override
    protected String name() {
        return NAME;
    }

    @Override
    protected String description() {
        return "Lists the gateway's virtual topics, with the physical topic each maps to, its consume "
                + "filter and value format, and the physical topics on the broker, with partitions and "
                + "replication factor. Pass `type` to list only one layer, and `name` to list only the topics "
                + "whose name matches a regular expression.";
    }

    @Override
    protected String inputSchema() {
        return INPUT_SCHEMA;
    }

    @Override
    protected String outputSchema() {
        return ListTopicsView.SCHEMA;
    }

    @Override
    protected ListTopicsView call(CallToolRequest request) {
        var layer = layer(request);
        return ListTopicsView.of(topicService, namePattern(request), layer == null || VIRTUAL.equals(layer),
                layer == null || PHYSICAL.equals(layer));
    }

    /// The compiled `name` argument, or `null` when it is absent or blank and every topic is listed.
    private @Nullable Pattern namePattern(CallToolRequest request) {
        return getOptionalArg(request, "name").map(name -> {
            if (name.length() > MAX_NAME_PATTERN_LENGTH) {
                throw new IllegalArgumentException("name must be at most " + MAX_NAME_PATTERN_LENGTH + " characters");
            }
            try {
                return Pattern.compile(name);
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("name is not a valid regular expression: " + e.getDescription(), e);
            }
        }).orElse(null);
    }

    /// The `type` argument: `"virtual"`, `"physical"`, or `null` when absent or blank and both layers are listed.
    private @Nullable String layer(CallToolRequest request) {
        return getOptionalArg(request, "type").map(type -> {
            if (!VIRTUAL.equals(type) && !PHYSICAL.equals(type)) {
                throw new IllegalArgumentException("type must be 'virtual' or 'physical'");
            }
            return type;
        }).orElse(null);
    }
}
