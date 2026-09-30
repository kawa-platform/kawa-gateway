package io.jonasg.kawa.governance;

import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.jonasg.kawa.config.GovernanceRuleConfig.Expression;
import static io.jonasg.kawa.config.GovernanceRuleConfig.Selector;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GovernancePolicyTest {

    @Test
    void reloadCompilesValidRulesEagerly() {
        // given
        var config = new GovernanceConfig(Map.of(
                "min-partitions", new GovernanceRuleConfig(
                        "min-partitions",
                        "must have partitions",
                        "must have partitions",
                        Selector.topic(
                                Expression.cel("topic.partitions >= 1")
                        ),
                        Expression.cel("topic.partitions >= 1")
                )));

        // when / then
        assertThatCode(() -> new GovernancePolicy(config)).doesNotThrowAnyException();
    }

    @Test
    void reloadRejectsInvalidExpression() {
        // given
        var config = new GovernanceConfig(Map.of(
                "min-partitions", new GovernanceRuleConfig(
                        "min-partitions",
                        "must have partitions",
                        "must have partitions",
                        Selector.topic(
                                Expression.cel("topic.partitions >= 1")
                        ),
                        Expression.cel("topic.partitions >=")
                )));

        // when / then
        assertThatThrownBy(() -> new GovernancePolicy(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid governance rule 'min-partitions': Invalid CEL expression 'topic.partitions >='");
    }

    @Test
    void violationCarriesTheRulesErrorMessageNotItsDescription() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "min-partitions", new GovernanceRuleConfig(
                        "min-partitions",
                        "partitions must be at least 2",
                        "All topics must have at least 2 partitions.",
                        Selector.topic(Expression.cel("true")),
                        Expression.cel("topic.partitions >= 2")))));

        // when
        var violations = policy.evaluate("alice", "payments", new TopicSpec("orders", 1, 3, Map.of()));

        // then
        assertThat(violations).extracting(Violation::message)
                .containsExactly("partitions must be at least 2");
    }

    @Test
    void collectsAllViolations() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "min-partitions", rule("min-partitions", "too few partitions", "topic.partitions >= 12"),
                "max-partitions", rule("max-partitions", "too many partitions", "topic.partitions <= 3"),
                "cleanup", rule("cleanup", "must be compact", "topic.configs['cleanup.policy'] == 'compact'"))));

        // when
        var violations = policy.evaluate("alice", "payments", new TopicSpec("orders", 6, 3, Map.of()));

        // then
        assertThat(violations).extracting(Violation::rule)
                .containsExactlyInAnyOrder("min-partitions", "max-partitions", "cleanup");
        assertThat(violations).extracting(Violation::message)
                .contains("too few partitions", "too many partitions", "must be compact");
    }

    @Test
    void sortsViolationsByRuleName() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "zeta", rule("zeta", "z", "false"),
                "alpha", rule("alpha", "a", "false"),
                "mid", rule("mid", "m", "false"))));

        // when
        var violations = policy.evaluate("alice", "payments", new TopicSpec("orders", 6, 3, Map.of()));

        // then
        assertThat(violations).extracting(Violation::rule)
                .containsExactly("alpha", "mid", "zeta");
    }

    @Test
    void throwingRuleFailsClosed() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "broken", rule("broken", "broken rule", "topic.nonexistent == 'x'"))));

        // when / then
        assertThatThrownBy(() -> policy.evaluate("alice", "payments", new TopicSpec("orders", 6, 3, Map.of())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("broken");
    }

    @Test
    void nonBooleanResultIsAViolation() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "returns-name", rule("returns-name", "must return boolean", "topic.name"))));

        // when
        var violations = policy.evaluate("alice", "payments", new TopicSpec("orders", 6, 3, Map.of()));

        // then
        assertThat(violations).extracting(Violation::rule).containsExactly("returns-name");
    }

    @Test
    void emptyRulesProduceNoViolations() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(null));

        // when
        var violations = policy.evaluate("alice", "payments", new TopicSpec("orders", 6, 3, Map.of()));

        // then
        assertThat(violations).isEmpty();
    }

    @Test
    void brokerDefaultFieldsAreExposed() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "default-partitions", rule("default-partitions", "partitions must be default", "topic.partitions == -1"),
                "default-replication", rule("default-replication", "replication must be default", "topic.replicationFactor == -1"))));

        // when
        var violations = policy.evaluate("alice", "payments", new TopicSpec("orders", -1, -1, Map.of()));

        // then
        assertThat(violations).isEmpty();
    }

    @Test
    void exposesPrincipalAndServiceBindings() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "principal", rule("principal", "must be alice", "principal == 'alice'"),
                "service", rule("service", "must be payments", "service == 'payments'"))));

        // when
        var violations = policy.evaluate("bob", "payments", new TopicSpec("orders", 6, 3, Map.of()));

        // then
        assertThat(violations).extracting(Violation::rule).containsExactly("principal");
    }

    @Test
    void inOperatorWorksOnConfigs() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "has-cleanup", rule("has-cleanup", "must set cleanup.policy", "'cleanup.policy' in topic.configs"))));

        // when
        var violations = policy.evaluate("alice", "payments",
                new TopicSpec("orders", 6, 3, Map.of("cleanup.policy", "compact")));

        // then
        assertThat(violations).isEmpty();
    }

    @Test
    void failedReloadLeavesPreviousSnapshotIntact() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "min-partitions", rule("min-partitions", "must have partitions", "topic.partitions >= 1"))));
        var broken = new GovernanceConfig(Map.of(
                "broken", rule("broken", "broken rule", "topic.partitions >=")));

        // when / then
        assertThatThrownBy(() -> policy.reload(broken))
                .isInstanceOf(IllegalArgumentException.class);

        // then - the old rules still evaluate
        assertThat(policy.evaluate("alice", "payments", new TopicSpec("orders", 6, 3, Map.of()))).isEmpty();
    }

    @Test
    void matchingExemptionSkipsItsRule() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "min-partitions", rule("min-partitions", "too few partitions", "topic.partitions >= 3",
                        exemption("streams-internal",
                                "principal.startsWith('streams-') && topic.name.endsWith('-changelog')")))));

        // when
        var violations = policy.evaluate("streams-app", "payments", new TopicSpec("orders-changelog", 1, 3, Map.of()));

        // then
        assertThat(violations).isEmpty();
    }

    @Test
    void exemptionOnlySkipsItsOwnRule() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "min-partitions", rule("min-partitions", "too few partitions", "topic.partitions >= 3",
                        exemption("streams-internal", "principal.startsWith('streams-')")),
                "min-replication", rule("min-replication", "too few replicas", "topic.replicationFactor >= 3"))));

        // when
        var violations = policy.evaluate("streams-app", "payments", new TopicSpec("orders-changelog", 1, 1, Map.of()));

        // then
        assertThat(violations).extracting(Violation::rule).containsExactly("min-replication");
    }

    @Test
    void nonMatchingExemptionKeepsRuleEnforced() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "min-partitions", rule("min-partitions", "too few partitions", "topic.partitions >= 3",
                        exemption("streams-internal", "principal.startsWith('streams-')")))));

        // when
        var violations = policy.evaluate("other-app", "payments", new TopicSpec("orders", 1, 3, Map.of()));

        // then
        assertThat(violations).extracting(Violation::rule).containsExactly("min-partitions");
    }

    @Test
    void anyMatchingExemptionSkipsTheRule() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "min-partitions", rule("min-partitions", "too few partitions", "topic.partitions >= 3",
                        exemption("streams-internal", "principal.startsWith('streams-')"),
                        exemption("mirror-maker", "principal == 'mm2'")))));

        // when
        var violations = policy.evaluate("mm2", "payments", new TopicSpec("orders", 1, 3, Map.of()));

        // then
        assertThat(violations).isEmpty();
    }

    @Test
    void throwingExemptionDoesNotApply() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "min-partitions", rule("min-partitions", "too few partitions", "topic.partitions >= 3",
                        exemption("broken", "topic.nonexistent == 'x'")))));

        // when
        var violations = policy.evaluate("alice", "payments", new TopicSpec("orders", 1, 3, Map.of()));

        // then
        assertThat(violations).extracting(Violation::rule).containsExactly("min-partitions");
    }

    @Test
    void nonBooleanExemptionDoesNotApply() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "min-partitions", rule("min-partitions", "too few partitions", "topic.partitions >= 3",
                        exemption("returns-name", "topic.name")))));

        // when
        var violations = policy.evaluate("alice", "payments", new TopicSpec("orders", 1, 3, Map.of()));

        // then
        assertThat(violations).extracting(Violation::rule).containsExactly("min-partitions");
    }

    @Test
    void reloadRejectsInvalidExemptionExpression() {
        // given
        var config = new GovernanceConfig(Map.of(
                "min-partitions", rule("min-partitions", "too few partitions", "topic.partitions >= 3",
                        exemption("broken", "principal =="))));

        // when / then
        assertThatThrownBy(() -> new GovernancePolicy(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid governance rule 'min-partitions': exemption 'broken': "
                        + "Invalid CEL expression 'principal =='");
    }

    @Test
    void validationErrorIsEmptyForValidExpression() {
        // when / then
        assertThat(GovernancePolicy.validationError("topic.partitions >= 1")).isEmpty();
    }

    @Test
    void validationErrorDescribesInvalidExpression() {
        // when / then
        assertThat(GovernancePolicy.validationError("topic.partitions >="))
                .isPresent()
                .hasValueSatisfying(message -> assertThat(message).contains("topic.partitions >="));
    }

    private static GovernanceRuleConfig rule(
            String name, String message, String expression, GovernanceRuleConfig.Exemption... exemptions) {
        return new GovernanceRuleConfig(
                name,
                message,
                message,
                Selector.topic(Expression.cel("true")),
                Expression.cel(expression),
                List.of(exemptions));
    }

    private static GovernanceRuleConfig.Exemption exemption(String name, String expression) {
        return new GovernanceRuleConfig.Exemption(name, "test exemption", Expression.cel(expression));
    }
}
