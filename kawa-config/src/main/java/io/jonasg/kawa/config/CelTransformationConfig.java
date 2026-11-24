package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.annotation.JsonDeserialize;

/// Replaces the record value with the result of a CEL (Common Expression Language) expression,
/// for mappings the predefined steps cannot express, e.g. `{"id": value.order_id, "total": value.total}`.
/// The expression has the same `key`, `value`, `headers` and `timestamp` bindings as [CelFilterConfig].
///
/// @param expression the CEL expression whose result becomes the new value
/// @param errorHeader see [TransformationConfig#errorHeader()]; `true` when omitted
public record CelTransformationConfig(
        String expression,
        @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = OmitTrue.class)
        @JsonDeserialize(using = StrictBooleanDeserializer.class) Boolean errorHeader
) implements TransformationConfig {

    public CelTransformationConfig {
        errorHeader = errorHeader == null ? Boolean.TRUE : errorHeader;
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("cel transformation requires an 'expression'");
        }
    }

    public CelTransformationConfig(String expression) {
        this(expression, true);
    }
}
