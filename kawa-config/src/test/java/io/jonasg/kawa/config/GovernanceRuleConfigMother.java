package io.jonasg.kawa.config;

import io.jonasg.kawa.config.GovernanceRuleConfig.Expression;
import io.jonasg.kawa.config.GovernanceRuleConfig.Selector;

/// Object Mother for [GovernanceRuleConfig]: `governanceRule().build()` returns a valid rule with
/// sensible defaults; override only what a test cares about via the `withX` methods.
public final class GovernanceRuleConfigMother {

    private GovernanceRuleConfigMother() {
    }

    public static Builder governanceRule() {
        return new Builder();
    }

    public static final class Builder {

        private String name = "min-partitions";
        private String errorMessage = "must have at least one partition";
        private String description = "Topics must have at least one partition.";
        private Selector selector = Selector.topic(Expression.cel("true"));
        private Expression expression = Expression.cel("topic.partitions >= 1");

        private Builder() {
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder withSelector(Selector selector) {
            this.selector = selector;
            return this;
        }

        public Builder withExpression(Expression expression) {
            this.expression = expression;
            return this;
        }

        public GovernanceRuleConfig build() {
            return new GovernanceRuleConfig(name, errorMessage, description, selector, expression);
        }
    }
}
