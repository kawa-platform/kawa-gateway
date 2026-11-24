package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.annotation.JsonDeserialize;

/// Filter that evaluates a CEL (Common Expression Language) expression against each record.
/// The expression has access to `key`, `value`, `headers`, and `timestamp` bindings.
///
/// @param expression  the CEL expression to evaluate — must return a boolean
/// @param errorHeader see [VirtualTopicFilterConfig#errorHeader()]; `true` when omitted
public record CelFilterConfig(
        String expression,
        @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = OmitTrue.class)
        @JsonDeserialize(using = StrictBooleanDeserializer.class) Boolean errorHeader
) implements VirtualTopicFilterConfig {

    public CelFilterConfig {
        errorHeader = errorHeader == null ? Boolean.TRUE : errorHeader;
    }

    public CelFilterConfig(String expression) {
        this(expression, true);
    }
}
