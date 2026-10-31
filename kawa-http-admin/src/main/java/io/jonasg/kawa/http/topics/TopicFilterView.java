package io.jonasg.kawa.http.topics;

/// A structured consume-filter entry in the admin `/topics` response,
/// mirroring the frontend `TopicFilter` type.
///
/// @param kind       one of `"cel"`, `"header"`, `"headerContains"`, `"headerStartsWith"`, `"headerMatches"`
/// @param expression the CEL expression, the `header=value` pair for headerEquals, or a
///                   `header <operator> value` description for the other header predicates
record TopicFilterView(String kind, String expression) {
}
