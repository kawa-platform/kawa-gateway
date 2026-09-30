package io.jonasg.kawa.config;

import io.jonasg.kawa.config.GovernanceRuleConfig.Expression;
import io.jonasg.kawa.config.GovernanceRuleConfig.Selector;
import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.config.GovernanceRuleConfigMother.governanceRule;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GovernanceRuleConfigTest {

    @Test
    void rejectsNullName() {
        assertThatThrownBy(() -> governanceRule().withName(null).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name");
    }

    @Test
    void rejectsBlankName() {
        assertThatThrownBy(() -> governanceRule().withName("   ").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name");
    }

    @Test
    void rejectsNullExpression() {
        assertThatThrownBy(() -> governanceRule().withExpression(null).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expression");
    }

    @Test
    void acceptsValidConfig() {
        // when
        GovernanceRuleConfig config = governanceRule()
                .withName("min-partitions")
                .withErrorMessage("must have at least one partition")
                .withDescription("Topics must have at least one partition.")
                .withSelector(Selector.topic(Expression.cel("true")))
                .withExpression(Expression.cel("topic.partitions >= 1"))
                .build();

        // then
        assertThat(config.name()).isEqualTo("min-partitions");
        assertThat(config.errorMessage()).isEqualTo("must have at least one partition");
        assertThat(config.description()).isEqualTo("Topics must have at least one partition.");
        assertThat(config.selector()).isEqualTo(Selector.topic(Expression.cel("true")));
        assertThat(config.expression()).isEqualTo(Expression.cel("topic.partitions >= 1"));
    }

    @Test
    void nullExemptionsCoalesceToEmpty() {
        // when
        var config = new GovernanceRuleConfig(
                "min-partitions",
                "must have at least one partition",
                null,
                Selector.topic(),
                Expression.cel("topic.partitions >= 1"),
                null);

        // then
        assertThat(config.exemptions()).isEmpty();
    }
}
