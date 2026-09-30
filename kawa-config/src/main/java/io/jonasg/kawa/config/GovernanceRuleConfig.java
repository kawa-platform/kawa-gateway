package io.jonasg.kawa.config;

import org.apache.kafka.common.resource.ResourceType;

import java.util.List;

/// A single topic governance rule: a human-readable unique [name],
/// an [errorMessage] shown when the rules are violated, a [description] explaining the rule
/// in more detail, a [selector] that determines the Kafka Resource(s) the rule applies to and
/// an [expression] that evaluates to `true` when the topic is compliant.
///
/// @param name         human-readable unique name of the rule
/// @param errorMessage human-readable message shown when the rule is violated
/// @param description  human-readable description of the rule
/// @param selector     determines the Kafka Resource(s) the rule applies to
/// @param expression   expression that evaluates to `true` when the topic is compliant
/// @param exemptions   named cases this rule does not apply to; never `null`, empty when there are none
public record GovernanceRuleConfig(
        String name,
        String errorMessage,
        String description,
        Selector selector,
        Expression expression,
        List<Exemption> exemptions
) {

    /// A rule without exemptions.
    public GovernanceRuleConfig(
            String name,
            String errorMessage,
            String description,
            Selector selector,
            Expression expression
    ) {
        this(name, errorMessage, description, selector, expression, List.of());
    }

    /// A named case the rule does not apply to: when [#expression] evaluates to `true` for a
    /// request, the rule is skipped for that request.
    ///
    /// @param name        unique name of the exemption within its rule
    /// @param description why the exemption exists
    /// @param expression  expression that evaluates to `true` when the rule should be skipped
    public record Exemption(
            String name,
            String description,
            Expression expression
    ) {
    }

    public record Selector(
            ResourceType resourceType,
            Expression expression
    ) {
        public static Selector topic(Expression exp) {
            return new Selector(ResourceType.TOPIC, exp);
        }

        public static Selector topic() {
            return new Selector(ResourceType.TOPIC, null);
        }
    }

    public record Expression(
            Type type,
            String value
    ) {
        public enum Type {
            CEL
        }

        public static Expression cel(String value) {
            return new Expression(Type.CEL, value);
        }
    }

    public GovernanceRuleConfig {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be null or blank");
        }
        if (expression == null) {
            throw new IllegalArgumentException("expression must not be null or blank");
        }
        exemptions = exemptions == null ? List.of() : List.copyOf(exemptions);
    }
}
