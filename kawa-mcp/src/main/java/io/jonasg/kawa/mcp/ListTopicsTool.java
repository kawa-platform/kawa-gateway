package io.jonasg.kawa.mcp;

import io.jonasg.kawa.config.AvroFormatConfig;
import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.HeaderContainsFilterConfig;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.HeaderMatchesFilterConfig;
import io.jonasg.kawa.config.HeaderStartsWithFilterConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.PayloadFormatConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import io.jonasg.kawa.http.topics.TopicService;
import io.jonasg.kawa.mcp.TopicsView.FilterView;
import io.jonasg.kawa.mcp.TopicsView.PhysicalTopicView;
import io.jonasg.kawa.mcp.TopicsView.VirtualTopicView;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpStatelessServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/// The `list_topics` tool: the gateway's virtual topics and the broker's physical topics, read from
/// the [TopicService] and answered as a [TopicsView], both as structured content and as its JSON text.
/// The optional `name` argument is a regular expression searched anywhere in the name; a virtual topic
/// matches by its virtual name, a physical topic by its physical name.
final class ListTopicsTool {

    static final String NAME = "list_topics";

    static final int MAX_NAME_PATTERN_LENGTH = 256;

    private static final String VIRTUAL = "virtual";
    private static final String PHYSICAL = "physical";

    private static final Map<String, Object> INPUT_SCHEMA = Map.of(
            "type", "object",
            "properties", Map.of(
                    "name", Map.of(
                            "type", "string",
                            "description", "Optional case-sensitive regular expression, at most " + MAX_NAME_PATTERN_LENGTH
                                    + " characters, searched anywhere in the name. Omit or leave blank to list every topic."),
                    "type", Map.of(
                            "type", "string",
                            "description", "Optional layer to list: 'virtual' for the gateway's virtual topics, "
                                    + "'physical' for the broker's topics. Omit or leave blank to list both. "
                                    + "A layer that is not requested is returned as an empty list.")));

    private final TopicService topics;
    private final McpJsonMapper mapper;

    ListTopicsTool(TopicService topics, McpJsonMapper mapper) {
        this.topics = topics;
        this.mapper = mapper;
    }

    SyncToolSpecification specification() {
        var tool = Tool.builder(NAME, INPUT_SCHEMA)
                .description("Lists the gateway's virtual topics, with the physical topic each maps to, its consume "
                        + "filter and value format, and the physical topics on the broker, with partitions and "
                        + "replication factor. Pass `type` to list only one layer, and `name` to list only the topics "
                        + "whose name matches a regular expression.")
                .outputSchema(TopicsView.SCHEMA)
                .build();
        return SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((context, request) -> ToolResults.guarded(NAME, context, () -> call(request)))
                .build();
    }

    private CallToolResult call(CallToolRequest request) {
        var view = topicsView(namePattern(request), layer(request));
        try {
            return CallToolResult.builder()
                    .structuredContent(view)
                    .addTextContent(mapper.writeValueAsString(view))
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /// The compiled `name` argument, or `null` when it is absent or blank and every topic is listed.
    private static @Nullable Pattern namePattern(CallToolRequest request) {
        var arguments = request.arguments();
        var argument = arguments == null ? null : arguments.get("name");
        if (argument == null) {
            return null;
        }
        if (!(argument instanceof String name)) {
            throw new IllegalArgumentException("name must be a string");
        }
        if (name.isBlank()) {
            return null;
        }
        if (name.length() > MAX_NAME_PATTERN_LENGTH) {
            throw new IllegalArgumentException("name must be at most " + MAX_NAME_PATTERN_LENGTH + " characters");
        }
        try {
            return Pattern.compile(name);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("name is not a valid regular expression: " + e.getDescription(), e);
        }
    }

    /// The `type` argument: `"virtual"`, `"physical"`, or `null` when absent or blank and both layers are listed.
    private static @Nullable String layer(CallToolRequest request) {
        var arguments = request.arguments();
        var argument = arguments == null ? null : arguments.get("type");
        if (argument == null) {
            return null;
        }
        if (!(argument instanceof String type)) {
            throw new IllegalArgumentException("type must be 'virtual' or 'physical'");
        }
        if (type.isBlank()) {
            return null;
        }
        if (!VIRTUAL.equals(type) && !PHYSICAL.equals(type)) {
            throw new IllegalArgumentException("type must be 'virtual' or 'physical'");
        }
        return type;
    }

    private static boolean matches(@Nullable Pattern pattern, String name) {
        return pattern == null || pattern.matcher(name).find();
    }

    private TopicsView topicsView(@Nullable Pattern pattern, @Nullable String layer) {
        var listing = topics.listTopics();
        var virtualTopics = layer == null || VIRTUAL.equals(layer)
                ? listing.virtual().stream()
                        .filter(entry -> matches(pattern, entry.name()))
                        .map(entry -> new VirtualTopicView(
                                entry.name(),
                                entry.physicalTopic(),
                                topics.partitionCount(entry.physicalTopic()),
                                topics.replicationFactor(entry.physicalTopic()),
                                filterView(entry.filter()),
                                valueFormat(entry.valueFormat()),
                                entry.exposePhysicalTopic()))
                        .toList()
                : List.<VirtualTopicView>of();
        var physicalTopics = layer == null || PHYSICAL.equals(layer)
                ? listing.physical().stream()
                        .filter(topic -> matches(pattern, topic.name()))
                        .map(topic -> new PhysicalTopicView(
                                topic.name(),
                                topics.partitionCount(topic.name()),
                                topics.replicationFactor(topic.name())))
                        .toList()
                : List.<PhysicalTopicView>of();
        return new TopicsView(virtualTopics, physicalTopics);
    }

    private static @Nullable FilterView filterView(@Nullable VirtualTopicFilterConfig filter) {
        return switch (filter) {
            case null -> null;
            case HeaderEqualsFilterConfig f -> new FilterView("headerEquals", header(f.header(), "equals", f.value()));
            case HeaderContainsFilterConfig f -> new FilterView("headerContains", header(f.header(), "contains", f.value()));
            case HeaderStartsWithFilterConfig f ->
                    new FilterView("headerStartsWith", header(f.header(), "starts with", f.value()));
            case HeaderMatchesFilterConfig f -> new FilterView("headerMatches", header(f.header(), "matches", f.value()));
            case CelFilterConfig f -> new FilterView("cel", f.expression());
        };
    }

    private static String header(String header, String operator, String value) {
        return "header '" + header + "' " + operator + " '" + value + "'";
    }

    private static String valueFormat(@Nullable PayloadFormatConfig format) {
        return switch (format) {
            case null -> "raw";
            case JsonFormatConfig _ -> "json";
            case AvroFormatConfig _ -> "avro";
        };
    }
}
