package io.jonasg.kawa.config;

/// Replaces the record value with the result of a CEL (Common Expression Language) expression,
/// for mappings the predefined steps cannot express, e.g. `{"id": value.order_id, "total": value.total}`.
/// The expression has the same `key`, `value`, `headers` and `timestamp` bindings as [CelFilterConfig].
///
/// @param expression the CEL expression whose result becomes the new value
public record CelTransformationConfig(String expression) implements TransformationConfig {

    public CelTransformationConfig {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("cel transformation requires an 'expression'");
        }
    }
}
