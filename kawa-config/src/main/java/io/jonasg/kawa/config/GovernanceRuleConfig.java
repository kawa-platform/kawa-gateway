package io.jonasg.kawa.config;

import org.apache.kafka.common.resource.ResourceType;

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
public record GovernanceRuleConfig(
        String name,
        String errorMessage,
        String description,
        Selector selector,
        Expression expression
) {

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
    }
}
