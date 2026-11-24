package io.jonasg.kawa.http.topics;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.jonasg.kawa.config.OmitTrue;

/// A structured consume-filter entry in the admin `/topics` response,
/// mirroring the frontend `TopicFilter` type.
///
/// @param kind        one of `"cel"`, `"header"`, `"headerContains"`, `"headerStartsWith"`, `"headerMatches"`
/// @param expression  the CEL expression, the `header=value` pair for headerEquals, or a
///                    `header <operator> value` description for the other header predicates
/// @param errorHeader whether a record the filter cannot decode, delivered under `onDecodeError: include`,
///                    carries the `kawa-error` headers; left out when it does (the default)
record TopicFilterView(
        String kind,
        String expression,
        @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = OmitTrue.class) boolean errorHeader
) {
}
