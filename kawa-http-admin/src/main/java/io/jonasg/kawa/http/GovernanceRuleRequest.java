package io.jonasg.kawa.http;

import org.jspecify.annotations.NullUnmarked;

/// A single governance rule in a request body, mirroring the shape of
/// [io.jonasg.kawa.config.GovernanceRuleConfig] with every value still a raw string.
/// Validated and converted by [GovernanceConfigMapper] rather than by this record.
///
/// @param name         unique, human-readable name of the rule
/// @param errorMessage message shown when the rule rejects a resource
/// @param description  longer explanation of what the rule enforces
/// @param selector     which Kafka resources the rule applies to
/// @param expression   expression that must evaluate to `true` for the resource to be compliant
@NullUnmarked
public record GovernanceRuleRequest(
        String name,
        String errorMessage,
        String description,
        Selector selector,
        Expression expression
) {

    /// @param resourceType Kafka resource type the rule targets, e.g. `TOPIC`
    /// @param expression   narrows the selected resources; `null` selects them all
    @NullUnmarked
    public record Selector(
            String resourceType,
            Expression expression
    ) {
    }

    /// @param type  expression language, e.g. `CEL`
    /// @param value the expression source
    @NullUnmarked
    public record Expression(
            String type,
            String value
    ) {
    }
}
