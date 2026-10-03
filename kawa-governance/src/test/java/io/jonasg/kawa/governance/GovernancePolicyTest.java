package io.jonasg.kawa.governance;

import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.GovernanceVariableConfig;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.jonasg.kawa.config.GovernanceRuleConfig.Expression;
import static io.jonasg.kawa.config.GovernanceRuleConfig.Match;
import static io.jonasg.kawa.config.GovernanceRuleConfig.Selector;
import static io.jonasg.kawa.config.GovernanceRuleConfig.SubRule;
import static io.jonasg.kawa.config.GovernanceRuleConfig.TopicScope;
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
                // int('orders') cannot convert: a genuine evaluation error, not a missing field
                "broken", rule("broken", "broken rule", "int(topic.name) > 0"))));

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
    void numericConfigsConvertWithInt() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "retention", rule("retention", "keep a day", "int(topic.configs['retention.ms']) >= 86400000"))));

        // when
        var kept = policy.evaluate("alice", "svc", new TopicSpec("orders", 1, 3, Map.of("retention.ms", "172800000")));
        var tooShort = policy.evaluate("alice", "svc", new TopicSpec("orders", 1, 3, Map.of("retention.ms", "1000")));

        // then
        assertThat(kept)
                .withFailMessage(() -> "int() of a config value did not convert: " + kept)
                .isEmpty();
        assertThat(tooShort).extracting(Violation::rule).containsExactly("retention");
    }

    @Test
    void unknownResourceFieldIsACompileError() {
        // when / then
        assertThatThrownBy(() -> new GovernancePolicy(new GovernanceConfig(Map.of("typo", rule("typo", "m", "topic.partition >= 1")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid governance rule 'typo'");
    }

    @Test
    void inOperatorIsFalseForAConfigThatIsNotSet() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "no-cleanup", rule("no-cleanup", "must not set cleanup.policy", "!('cleanup.policy' in topic.configs)"))));

        // when
        var violations = policy.evaluate("alice", "payments", new TopicSpec("orders", 6, 3, Map.of()));

        // then
        assertThat(violations)
                .withFailMessage(() -> "'in' reported an unset config as present: " + violations)
                .isEmpty();
    }

    @Test
    void readingAConfigThatIsNotSetFailsTheCheckWithoutAnError() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "compact", rule("compact", "must be compact", "topic.configs['cleanup.policy'] == 'compact'"))));

        // when
        var trace = policy.dryRun(GovernanceRequest.topic("alice", "payments", new TopicSpec("orders", 6, 3, Map.of())));

        // then
        assertThat(trace.rules()).extracting(GovernanceTrace.RuleTrace::outcome).containsExactly(GovernanceTrace.Outcome.FAIL);
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
                        exemption("broken", "int(topic.name) > 0")))));

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

    @Test
    void subRulesUnderAnyPassWhenOneBranchHolds() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of("naming", namingRule())));

        // when
        var changelog = policy.evaluate("alice", "svc", new TopicSpec("app.cargo-flights-changelog", 1, 3, Map.of()));
        var unknown = policy.evaluate("alice", "svc", new TopicSpec("misc.x", 1, 3, Map.of()));

        // then
        assertThat(changelog)
                .withFailMessage(() -> "Topic matching the nested 'changelog' check was refused: " + changelog)
                .isEmpty();
        assertThat(unknown).extracting(Violation::rule).containsExactly("naming");
    }

    @Test
    void ruleIsSkippedWhenItsSelectorDoesNotHold() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of("model-compacted", new GovernanceRuleConfig(
                "model-compacted", "model topics must be compacted", null,
                Selector.topic(Expression.cel("topic.name.startsWith('model.')")),
                Expression.cel("topic.configs['cleanup.policy'] == 'compact'")))));

        // when
        var other = policy.evaluate("alice", "svc", new TopicSpec("app.x", 1, 3, Map.of()));
        var model = policy.evaluate("alice", "svc", new TopicSpec("model.x", 1, 3, Map.of()));

        // then
        assertThat(other)
                .withFailMessage(() -> "Rule was applied although its selector is false: " + other)
                .isEmpty();
        assertThat(model).extracting(Violation::rule).containsExactly("model-compacted");
    }

    @Test
    void virtualOnlyAndNonTopicRulesAreNotEnforcedYet() {
        // given
        var never = Expression.cel("false");
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "virtual-only", new GovernanceRuleConfig("virtual-only", "m", null,
                        new Selector(ResourceType.TOPIC, null, TopicScope.VIRTUAL), never),
                "groups", new GovernanceRuleConfig("groups", "m", null,
                        new Selector(ResourceType.GROUP, null), Expression.cel("group.id.startsWith('app.')")))));

        // when
        var violations = policy.evaluate("alice", "svc", new TopicSpec("orders", 1, 3, Map.of()));

        // then
        assertThat(violations)
                .withFailMessage(() -> "A virtual-only or group rule was enforced on a physical topic: " + violations)
                .isEmpty();
    }

    @Test
    void topicVirtualIsBoundFalseForPhysicalTopics() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "physical-guard", rule("physical-guard", "m", "!topic.virtual && topic.partitions >= 1"))));

        // when
        var violations = policy.evaluate("alice", "svc", new TopicSpec("orders", 3, 3, Map.of()));

        // then
        assertThat(violations)
                .withFailMessage(() -> "Guarded rule failed on a physical topic: " + violations)
                .isEmpty();
    }

    @Test
    void globalExemptionSkipsEveryRule() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(
                Map.of("never", rule("never", "never passes", "false")),
                Map.of("platform", exemption("platform", "principal.startsWith('User:platform-')"))));

        // when
        var exempt = policy.evaluate("User:platform-ops", "svc", new TopicSpec("orders", 1, 3, Map.of()));
        var other = policy.evaluate("User:app", "svc", new TopicSpec("orders", 1, 3, Map.of()));

        // then
        assertThat(exempt)
                .withFailMessage(() -> "Global exemption did not skip the rules: " + exempt)
                .isEmpty();
        assertThat(other).extracting(Violation::rule).containsExactly("never");
    }

    @Test
    void globalExemptionReadingAnotherResourceDoesNotApply() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(
                Map.of("never", rule("never", "never passes", "false")),
                Map.of("groups", exemption("groups", "group.id.startsWith('connect-')"))));

        // when
        var violations = policy.evaluate("alice", "svc", new TopicSpec("orders", 1, 3, Map.of()));

        // then
        assertThat(violations).extracting(Violation::rule).containsExactly("never");
    }

    @Test
    void reloadRejectsRuleReadingAnotherResourceVariable() {
        // given
        var config = new GovernanceConfig(Map.of("r", rule("r", "m", "group.id == 'x'")));

        // when / then
        assertThatThrownBy(() -> new GovernancePolicy(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid governance rule 'r'");
    }

    @Test
    void rulesReadVariablesByName() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(
                Map.of("tiers", rule("tiers", "partitions must match a tier", "topic.partitions in partitionTiers")),
                Map.of(),
                Map.of("partitionTiers", new GovernanceVariableConfig("partitionTiers", GovernanceVariableConfig.Type.LIST_INT, "[1, 4, 6, 12]", null))));

        // when
        var inTier = policy.evaluate("alice", "svc", new TopicSpec("orders", 6, 3, Map.of()));
        var offTier = policy.evaluate("alice", "svc", new TopicSpec("orders", 5, 3, Map.of()));

        // then
        assertThat(inTier)
                .withFailMessage(() -> "6 partitions was refused although it is in partitionTiers: " + inTier)
                .isEmpty();
        assertThat(offTier).extracting(Violation::rule).containsExactly("tiers");
    }

    @Test
    void regexVariableWithRepeatedNamedGroupsMatchesAsPlainGroups() {
        // given
        var pattern = "\"^(?:model\\\\.(?<domain>[a-z]+)|event\\\\.(?<domain>[a-z]+))$\"";
        var policy = new GovernancePolicy(new GovernanceConfig(
                Map.of("naming", rule("naming", "bad name", "topic.name.matches(namingPattern)")),
                Map.of(),
                Map.of("namingPattern", new GovernanceVariableConfig("namingPattern", GovernanceVariableConfig.Type.STRING, pattern, null))));

        // when
        var violations = policy.evaluate("alice", "svc", new TopicSpec("event.orders", 1, 3, Map.of()));

        // then
        assertThat(violations)
                .withFailMessage(() -> "Topic matching the second branch was refused: " + violations)
                .isEmpty();
    }

    @Test
    void validateRefusesRuleReadingUnknownVariable() {
        // given
        var config = new GovernanceConfig(Map.of("tiers", rule("tiers", "m", "topic.partitions in partitionTiers")));

        // when / then
        assertThatThrownBy(() -> GovernancePolicy.validate(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid governance rule 'tiers'");
    }

    @Test
    void variableValueRejectsLiteralOfAnotherType() {
        // given
        var variable = new GovernanceVariableConfig("tiers", GovernanceVariableConfig.Type.LIST_INT, "[\"a\"]", null);

        // when / then
        assertThatThrownBy(() -> GovernancePolicy.variableValue(variable))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("governance variable 'tiers': value is not a list<int>");
    }

    @Test
    void dryRunTracesEveryCheckWithShortCircuiting() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of("naming", namingRule())));

        // when
        var trace = policy.dryRun(GovernanceRequest.topic("alice", "svc", new TopicSpec("app.cargo-flights-changelog", 1, 3, Map.of())));

        // then
        assertThat(trace.allowed()).isTrue();
        var rule = trace.rules().getFirst();
        assertThat(rule.subRules()).extracting(GovernanceTrace.NodeTrace::outcome)
                .containsExactly(GovernanceTrace.Outcome.FAIL, GovernanceTrace.Outcome.PASS);
        assertThat(rule.subRules().get(1).checks()).extracting(GovernanceTrace.NodeTrace::outcome)
                .containsExactly(GovernanceTrace.Outcome.FAIL, GovernanceTrace.Outcome.PASS);
    }

    @Test
    void violationNamesTheFailingCheckAndItsNearestMessage() {
        // given
        var rule = new GovernanceRuleConfig("model", "model topics are invalid", null,
                Selector.topic(Expression.cel("topic.name.startsWith('model.')")),
                Match.ALL,
                List.of(new SubRule.Group("compaction", "model topics must be compacted", Match.ALL, List.of(
                        SubRule.check("policy-set", Expression.cel("'cleanup.policy' in topic.configs")),
                        SubRule.check("compact", Expression.cel("topic.configs['cleanup.policy'] == 'compact'"))))),
                List.of());
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of("model", rule)));

        // when
        var violations = policy.evaluate("alice", "svc", new TopicSpec("model.x", 1, 3, Map.of("cleanup.policy", "delete")));

        // then
        assertThat(violations).containsExactly(
                new Violation("model", "model topics must be compacted", List.of("compaction", "compact")));
    }

    @Test
    void virtualRulesJudgeVirtualTopics() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of("virtual", new GovernanceRuleConfig("virtual", "m", null,
                new Selector(ResourceType.TOPIC, null, TopicScope.VIRTUAL), Expression.cel("topic.name.startsWith('app.')")))));

        // when
        var trace = policy.dryRun(new GovernanceRequest(ResourceType.TOPIC, null, true,
                Map.of("name", "billing", "physicalTopic", "app.billing"), "alice", "svc"));

        // then
        assertThat(trace.allowed()).isFalse();
        assertThat(trace.rules()).extracting(GovernanceTrace.RuleTrace::outcome).containsExactly(GovernanceTrace.Outcome.FAIL);
    }

    @Test
    void groupRulesAreEnforced() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of("groups", groupRule("group.id.startsWith('app.')"))));

        // when
        var trace = policy.dryRun(GovernanceRequest.group("alice", "kafka", "billing"));

        // then
        assertThat(policy.evaluate(GovernanceRequest.group("alice", "kafka", "billing")))
                .extracting(Violation::describe).containsExactly("[groups] m");
    }

    @Test
    void operationsDoNotNarrowGroupRules() {
        // given
        var rule = new GovernanceRuleConfig("groups", "m", null,
                new Selector(ResourceType.GROUP, null, TopicScope.BOTH, Set.of(GovernanceRuleConfig.Operation.ALTER)),
                Expression.cel("group.id.startsWith('app.')"));
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of("groups", rule)));

        // when
        var violations = policy.evaluate(GovernanceRequest.group("alice", "kafka", "billing"));

        // then
        assertThat(violations)
                .withFailMessage(() -> "A group rule running on ALTER only was skipped for a group request")
                .hasSize(1);
    }

    @Test
    void transactionRulesBindTheTransactionalId() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of("txn", new GovernanceRuleConfig("txn", "m", null,
                new Selector(ResourceType.TRANSACTIONAL_ID, null), Expression.cel("transaction.id.endsWith('-tx')")))));

        // when / then
        assertThat(policy.evaluate(GovernanceRequest.transaction("alice", "kafka", "app-tx"))).isEmpty();
        assertThat(policy.evaluate(GovernanceRequest.transaction("alice", "kafka", "app"))).hasSize(1);
    }

    @Test
    void reloadDropsCachedGroupVerdicts() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of("groups", groupRule("group.id.startsWith('app.')"))));
        var request = GovernanceRequest.group("alice", "kafka", "billing");
        assertThat(policy.evaluate(request)).hasSize(1);

        // when
        policy.reload(new GovernanceConfig(Map.of("groups", groupRule("true"))));

        // then
        assertThat(policy.evaluate(request))
                .withFailMessage(() -> "A verdict cached before the reload was served after it")
                .isEmpty();
    }

    @Test
    void knowsWhetherAnyRuleRunsOnPhysicalTopicChanges() {
        // given
        var createOnly = new GovernancePolicy(new GovernanceConfig(Map.of("naming", namingRule())));
        var virtualAlter = new GovernancePolicy(new GovernanceConfig(Map.of("v", new GovernanceRuleConfig("v", "m", null,
                new Selector(ResourceType.TOPIC, null, TopicScope.VIRTUAL, Set.of(GovernanceRuleConfig.Operation.ALTER)),
                Expression.cel("true")))));
        var physicalAlter = new GovernancePolicy(new GovernanceConfig(Map.of("p", new GovernanceRuleConfig("p", "m", null,
                new Selector(ResourceType.TOPIC, null, TopicScope.BOTH, Set.of(GovernanceRuleConfig.Operation.ALTER)),
                Expression.cel("true")))));

        // when / then
        assertThat(createOnly.hasPhysicalTopicRules(GovernanceRuleConfig.Operation.ALTER)).isFalse();
        assertThat(virtualAlter.hasPhysicalTopicRules(GovernanceRuleConfig.Operation.ALTER)).isFalse();
        assertThat(physicalAlter.hasPhysicalTopicRules(GovernanceRuleConfig.Operation.ALTER)).isTrue();
    }

    private static GovernanceRuleConfig groupRule(String expression) {
        return new GovernanceRuleConfig("groups", "m", null, new Selector(ResourceType.GROUP, null), Expression.cel(expression));
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

    /// `app.<word>`, or a hyphenated `app.` name ending in `-events` or `-changelog`.
    private static GovernanceRuleConfig namingRule() {
        return new GovernanceRuleConfig("naming", "topic names follow the app convention", null,
                Selector.topic(), Match.ANY,
                List.of(
                        SubRule.check("plain", Expression.cel("topic.name.matches('^app\\\\.[a-z]+$')")),
                        SubRule.group("suffixed", Match.ANY, List.of(
                                SubRule.check("events", Expression.cel("topic.name.matches('^app\\\\.[a-z-]+-events$')")),
                                SubRule.check("changelog", Expression.cel("topic.name.matches('^app\\\\.[a-z-]+-changelog$')"))))),
                List.of());
    }

    private static GovernanceRuleConfig.Exemption exemption(String name, String expression) {
        return new GovernanceRuleConfig.Exemption(name, "test exemption", Expression.cel(expression));
    }
}
