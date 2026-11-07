package io.jonasg.kawa.mcp;

/// A virtual topic's consume filter: only records it matches are served. Shared by the `list_topics` and
/// `describe_topic` views, so neither tool depends on the other.
///
/// @param type        the filter kind, as in the config: `headerEquals`, `headerContains`,
///                    `headerStartsWith`, `headerMatches` or `cel`
/// @param description what it matches, e.g. `header 'region' equals 'eu'` or the CEL expression
public record FilterView(String type, String description) {
}
