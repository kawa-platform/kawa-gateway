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
    void nullRulesAndExemptionsCoalesceToEmpty() {
        // when
        GovernanceConfig config = new GovernanceConfig(null, null);

        // then
        assertThat(config.rules()).isEmpty();
        assertThat(config.exemptions()).isEmpty();
    }

    @Test
    void copiesAreImmutable() {
        // given
        Map<String, GovernanceRuleConfig> rules = new HashMap<>(Map.of(
                "min-partitions", governanceRule().build()));
        Map<String, GovernanceExemptionConfig> exemptions = new HashMap<>(Map.of(
                "streams-internal", new GovernanceExemptionConfig("^streams-.*", ".*-changelog$")));

        // when
        GovernanceConfig config = new GovernanceConfig(rules, exemptions);

        // then
        assertThatThrownBy(() -> config.rules().put("x", null))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> config.exemptions().put("x", null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void upsertRuleAddsNewRule() {
        // given
        var config = new GovernanceConfig(null, null);

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
                governanceRule().withExpression(Expression.cel("true")).build()), null);
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
                governanceRule().build()), null);

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
                        .build()), null);

        // when
        var updated = config.removeRule("min-partitions");

        // then
        assertThat(updated.rules()).hasSize(1);
        assertThat(updated.rules()).containsKey("max-partitions");
    }

    @Test
    void upsertExemptionAddsNewExemption() {
        // given
        var config = new GovernanceConfig(null, null);

        // when
        var updated = config.upsertExemption("streams-internal",
                new GovernanceExemptionConfig("^streams-.*", ".*-changelog$"));

        // then
        assertThat(updated.exemptions()).containsEntry("streams-internal",
                new GovernanceExemptionConfig("^streams-.*", ".*-changelog$"));
    }

    @Test
    void upsertExemptionOverwritesExisting() {
        // given
        var config = new GovernanceConfig(null, Map.of("streams-internal",
                new GovernanceExemptionConfig("^old-.*", ".*")));
        var newExemption = new GovernanceExemptionConfig("^streams-.*", ".*-changelog$");

        // when
        var updated = config.upsertExemption("streams-internal", newExemption);

        // then
        assertThat(updated.exemptions()).containsEntry("streams-internal", newExemption);
        assertThat(updated.exemptions()).hasSize(1);
    }

    @Test
    void removeExemptionRemovesExisting() {
        // given
        var config = new GovernanceConfig(null, Map.of("streams-internal",
                new GovernanceExemptionConfig("^streams-.*", ".*-changelog$")));

        // when
        var updated = config.removeExemption("streams-internal");

        // then
        assertThat(updated.exemptions()).isEmpty();
    }

    @Test
    void removeExemptionPreservesOtherExemptions() {
        // given
        var config = new GovernanceConfig(null, Map.of(
                "streams-internal", new GovernanceExemptionConfig("^streams-.*", ".*-changelog$"),
                "mirror-maker", new GovernanceExemptionConfig("^mm2-.*", ".*")));

        // when
        var updated = config.removeExemption("streams-internal");

        // then
        assertThat(updated.exemptions()).hasSize(1);
        assertThat(updated.exemptions()).containsKey("mirror-maker");
    }

    @Test
    void upsertRulePreservesExemptions() {
        // given
        var config = new GovernanceConfig(
                Map.of("min-partitions", governanceRule().build()),
                Map.of("streams-internal", new GovernanceExemptionConfig("^streams-.*", ".*-changelog$")));

        // when
        var updated = config.upsertRule("max-partitions",
                governanceRule()
                        .withName("max-partitions")
                        .withErrorMessage("too many partitions")
                        .withExpression(Expression.cel("topic.partitions <= 12"))
                        .build());

        // then
        assertThat(updated.rules()).hasSize(2);
        assertThat(updated.exemptions()).containsEntry("streams-internal",
                new GovernanceExemptionConfig("^streams-.*", ".*-changelog$"));
    }
}
