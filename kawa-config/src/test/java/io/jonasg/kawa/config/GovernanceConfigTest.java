package io.jonasg.kawa.config;

import io.jonasg.kawa.config.GovernanceRuleConfig.Expression;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.jonasg.kawa.config.GovernanceRuleConfigMother.governanceRule;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GovernanceConfigTest {

    @Test
    void nullRulesCoalesceToEmpty() {
        // when
        GovernanceConfig config = new GovernanceConfig(null);

        // then
        assertThat(config.rules()).isEmpty();
    }

    @Test
    void copiesAreImmutable() {
        // given
        Map<String, GovernanceRuleConfig> rules = new HashMap<>(Map.of(
                "min-partitions", governanceRule().build()));

        // when
        GovernanceConfig config = new GovernanceConfig(rules);

        // then
        assertThatThrownBy(() -> config.rules().put("x", null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void upsertRuleAddsNewRule() {
        // given
        var config = new GovernanceConfig(null);

        // when
        var updated = config.upsertRule("min-partitions",
                governanceRule().build());

        // then
        assertThat(updated.rules()).containsEntry("min-partitions",
                governanceRule().build());
    }

    @Test
    void upsertRuleOverwritesExisting() {
        // given
        var config = new GovernanceConfig(Map.of("min-partitions",
                governanceRule().withExpression(Expression.cel("true")).build()));
        var newRule = governanceRule().build();

        // when
        var updated = config.upsertRule("min-partitions", newRule);

        // then
        assertThat(updated.rules()).containsEntry("min-partitions", newRule);
        assertThat(updated.rules()).hasSize(1);
    }

    @Test
    void removeRuleRemovesExisting() {
        // given
        var config = new GovernanceConfig(Map.of("min-partitions",
                governanceRule().build()));

        // when
        var updated = config.removeRule("min-partitions");

        // then
        assertThat(updated.rules()).isEmpty();
    }

    @Test
    void removeRulePreservesOtherRules() {
        // given
        var config = new GovernanceConfig(Map.of(
                "min-partitions", governanceRule().build(),
                "max-partitions", governanceRule()
                        .withName("max-partitions")
                        .withErrorMessage("too many partitions")
                        .withExpression(Expression.cel("topic.partitions <= 12"))
                        .build()));

        // when
        var updated = config.removeRule("min-partitions");

        // then
        assertThat(updated.rules()).hasSize(1);
        assertThat(updated.rules()).containsKey("max-partitions");
    }

    @Test
    void upsertRulePreservesOtherRules() {
        // given
        var config = new GovernanceConfig(
                Map.of("min-partitions", governanceRule().build()));

        // when
        var updated = config.upsertRule("max-partitions",
                governanceRule()
                        .withName("max-partitions")
                        .withErrorMessage("too many partitions")
                        .withExpression(Expression.cel("topic.partitions <= 12"))
                        .build());

        // then
        assertThat(updated.rules()).containsOnlyKeys("min-partitions", "max-partitions");
    }
}
