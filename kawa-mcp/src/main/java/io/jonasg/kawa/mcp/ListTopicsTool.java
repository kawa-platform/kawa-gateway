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
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

/// The `list_topics` tool: the gateway's virtual topics and the broker's physical topics, read from
/// the [TopicService] and answered as a [TopicsView], both as structured content and as its JSON text.
final class ListTopicsTool {

    static final String NAME = "list_topics";

    private static final Map<String, Object> NO_ARGUMENTS = Map.of("type", "object", "properties", Map.of());

    private final TopicService topics;
    private final McpJsonMapper mapper;

    ListTopicsTool(TopicService topics, McpJsonMapper mapper) {
        this.topics = topics;
        this.mapper = mapper;
    }

    SyncToolSpecification specification() {
        var tool = Tool.builder(NAME, NO_ARGUMENTS)
                .description("Lists the gateway's virtual topics, with the physical topic each maps to, its consume "
                        + "filter and value format, and the physical topics on the broker, with partitions and "
                        + "replication factor.")
                .outputSchema(TopicsView.SCHEMA)
                .build();
        return SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((_, _) -> ToolResults.guarded(NAME, this::call))
                .build();
    }

    private CallToolResult call() {
        var view = topicsView();
        try {
            return CallToolResult.builder()
                    .structuredContent(view)
                    .addTextContent(mapper.writeValueAsString(view))
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private TopicsView topicsView() {
        var listing = topics.listTopics();
        var virtualTopics = listing.virtual().stream()
                .map(entry -> new VirtualTopicView(
                        entry.name(),
                        entry.physicalTopic(),
                        topics.partitionCount(entry.physicalTopic()),
                        topics.replicationFactor(entry.physicalTopic()),
                        filterView(entry.filter()),
                        valueFormat(entry.valueFormat()),
                        entry.exposePhysicalTopic()))
                .toList();
        var physicalTopics = listing.physical().stream()
                .map(topic -> new PhysicalTopicView(
                        topic.name(),
                        topics.partitionCount(topic.name()),
                        topics.replicationFactor(topic.name())))
                .toList();
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
