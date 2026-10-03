package io.jonasg.kawa.config;

import io.jonasg.kawa.config.GovernanceRuleConfig.Expression;
import io.jonasg.kawa.config.GovernanceRuleConfig.Selector;
import io.jonasg.kawa.config.GovernanceRuleConfig.SubRule;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

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

    @Test
    void expressionCombinesSubRulesPerMatch() {
        // when
        var config = new GovernanceRuleConfig("naming", "msg", null, Selector.topic(), GovernanceRuleConfig.Match.ANY, List.of(
                SubRule.check("model", Expression.cel("a")),
                SubRule.group("app", GovernanceRuleConfig.Match.ALL, List.of(
                        SubRule.check("b", Expression.cel("b")),
                        SubRule.check("c", Expression.cel("c"))))), List.of());

        // then
        assertThat(config.expression()).isEqualTo(Expression.cel("(a) || ((b) && (c))"));
    }

    @Test
    void rejectsGroupWithoutChecks() {
        // when / then
        assertThatThrownBy(() -> SubRule.group("app", GovernanceRuleConfig.Match.ANY, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a group needs at least one check");
    }

    @Test
    void rejectsDuplicateCheckNamesInGroup() {
        // when / then
        assertThatThrownBy(() -> SubRule.group("app", GovernanceRuleConfig.Match.ANY, List.of(
                SubRule.check("a", Expression.cel("true")),
                SubRule.check("a", Expression.cel("false")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate sub-rule name 'a'");
    }

    @Test
    void readsLegacySingleExpressionAsOneCheck() {
        // given
        var json = """
                {
                  "name": "min-partitions",
                  "errorMessage": "msg",
                  "selector": {"resourceType": "TOPIC", "expression": null},
                  "expression": {"type": "CEL", "value": "topic.partitions >= 1"},
                  "exemptions": []
                }
                """;

        // when
        var config = JsonMapper.builder().build().readValue(json, GovernanceRuleConfig.class);

        // then
        assertThat(config.subRules()).containsExactly(SubRule.check("min-partitions", Expression.cel("topic.partitions >= 1")));
        assertThat(config.selector().scope()).isEqualTo(GovernanceRuleConfig.TopicScope.BOTH);
    }

    @Test
    void roundTripsSubRulesThroughJson() {
        // given
        var mapper = JsonMapper.builder().build();
        var config = new GovernanceRuleConfig("naming", "msg", null,
                new Selector(ResourceType.TOPIC, null, GovernanceRuleConfig.TopicScope.PHYSICAL),
                GovernanceRuleConfig.Match.ANY,
                List.of(
                        SubRule.check("model", Expression.cel("topic.name.startsWith('model.')")),
                        new SubRule.Group("app", "App topics only", GovernanceRuleConfig.Match.ALL,
                                List.of(SubRule.check("standard", Expression.cel("topic.name.startsWith('app.')"))))),
                List.of());

        // when
        var json = mapper.writeValueAsString(config);
        var read = mapper.readValue(json, GovernanceRuleConfig.class);

        // then
        assertThat(read).isEqualTo(config);
        assertThat(json).contains("\"kind\":\"group\"").contains("\"checks\"");
    }
}
