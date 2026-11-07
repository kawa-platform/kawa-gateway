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
import org.jspecify.annotations.Nullable;

/// How a virtual topic's consume filter and value format are shown to an agent. Shared by the `list_topics`
/// and `describe_topic` views, so both report a virtual topic the same way.
public final class VirtualTopicViews {

    private VirtualTopicViews() {
    }

    /// The consume filter, or `null` when every record is served.
    public static @Nullable FilterView filter(@Nullable VirtualTopicFilterConfig filter) {
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

    /// The value format: `json`, `avro`, or `raw` when record values are not decoded.
    public static String valueFormat(@Nullable PayloadFormatConfig format) {
        return switch (format) {
            case null -> "raw";
            case JsonFormatConfig _ -> "json";
            case AvroFormatConfig _ -> "avro";
        };
    }

    private static String header(String header, String operator, String value) {
        return "header '" + header + "' " + operator + " '" + value + "'";
    }
}
