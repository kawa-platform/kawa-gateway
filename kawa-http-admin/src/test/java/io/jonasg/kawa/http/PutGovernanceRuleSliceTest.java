package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig.Expression;
import io.jonasg.kawa.config.GovernanceRuleConfig.Selector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for [PutGovernanceRuleHandler]: real HTTP requests through a booted [AdminHttpServer],
/// asserting the JSON wire format the admin UI consumes and the snapshot that gets persisted.
class PutGovernanceRuleSliceTest extends AdminHttpSliceTestBase {

    @Test
    void addsRuleAndPersistsSnapshot() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", rulePath("event-topic-naming-convention"), """
                {
                  "name": "event-topic-naming-convention",
                  "errorMessage": "Event topics should start with event.",
                  "description": "Event topics are expected to start with event.",
                  "selector": {"resourceType": "TOPIC", "expression": {"type": "CEL", "value": "true"}},
                  "expression": {"type": "CEL", "value": "topic.name.startsWith('event.')"}
                }
                """);

        // then
        assertThat(response)
                .hasStatusCode(200)
                .hasBody("""
                {
                  "name": "event-topic-naming-convention",
                  "errorMessage": "Event topics should start with event.",
                  "description": "Event topics are expected to start with event.",
                  "selector": {"resourceType": "TOPIC", "expression": {"type": "CEL", "value": "true"}, "scope": "BOTH", "operations": ["CREATE"]},
                  "match": "ALL",
                  "subRules": [
                    {
                      "kind": "check",
                      "name": "event-topic-naming-convention",
                      "errorMessage": null,
                      "expression": {"type": "CEL", "value": "topic.name.startsWith('event.')"}
                    }
                  ],
                  "exemptions": []
                }
                """);
        assertThat(repository.getActiveConfig().governance().rules())
                .containsExactlyEntriesOf(Map.of("event-topic-naming-convention", new GovernanceRuleConfig(
                        "event-topic-naming-convention",
                        "Event topics should start with event.",
                        "Event topics are expected to start with event.",
                        Selector.topic(Expression.cel("true")),
                        Expression.cel("topic.name.startsWith('event.')"))));
    }

    @Test
    void addsRuleSelectingAllTopicsWhenSelectorHasNoExpression() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", rulePath("min-replication"), """
                {
                  "name": "min-replication",
                  "errorMessage": "replication factor must be at least 3",
                  "selector": {"resourceType": "TOPIC"},
                  "expression": {"type": "CEL", "value": "topic.replicationFactor >= 3"}
                }
                """);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                {
                  "name": "min-replication",
                  "errorMessage": "replication factor must be at least 3",
                  "description": null,
                  "selector": {"resourceType": "TOPIC", "expression": null, "scope": "BOTH", "operations": ["CREATE"]},
                  "match": "ALL",
                  "subRules": [
                    {
                      "kind": "check",
                      "name": "min-replication",
                      "errorMessage": null,
                      "expression": {"type": "CEL", "value": "topic.replicationFactor >= 3"}
                    }
                  ],
                  "exemptions": []
                }
                """);
        assertThat(repository.getActiveConfig().governance().rules().get("min-replication").selector())
                .isEqualTo(Selector.topic());
    }

    @Test
    void addsRuleWithExemptionsAndPersistsSnapshot() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", rulePath("min-partitions"), """
                {
                  "errorMessage": "partitions must be at least 2",
                  "selector": {"resourceType": "TOPIC"},
                  "expression": {"type": "CEL", "value": "topic.partitions >= 2"},
                  "exemptions": [
                    {
                      "name": "streams-internal",
                      "description": "Kafka Streams manages its own changelog topics.",
                      "expression": {"type": "CEL", "value": "principal.startsWith('streams-')"}
                    }
                  ]
                }
                """);

        // then
        assertThat(response)
                .hasStatusCode(200)
                .hasBody("""
                        {
                          "name": "min-partitions",
                          "errorMessage": "partitions must be at least 2",
                          "description": null,
                          "selector": {
                            "resourceType": "TOPIC",
                            "expression": null,
                            "scope": "BOTH",
                            "operations": ["CREATE"]
                          },
                          "match": "ALL",
                          "subRules": [
                            {
                              "kind": "check",
                              "name": "min-partitions",
                              "errorMessage": null,
                              "expression": {"type": "CEL", "value": "topic.partitions >= 2"}
                            }
                          ],
                          "exemptions": [
                            {
                              "name": "streams-internal",
                              "description": "Kafka Streams manages its own changelog topics.",
                              "expression": {
                                "type": "CEL",
                                "value": "principal.startsWith('streams-')"
                              }
                            }
                          ]
                        }
                        """);
        assertThat(repository.getActiveConfig().governance().rules().get("min-partitions").exemptions())
                .containsExactly(new GovernanceRuleConfig.Exemption(
                        "streams-internal",
                        "Kafka Streams manages its own changelog topics.",
                        Expression.cel("principal.startsWith('streams-')")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidExemptions")
    void rejectsInvalidExemption(String description, String exemptions, String expectedError) throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", rulePath("r"), """
                {
                  "errorMessage": "msg",
                  "selector": {"resourceType": "TOPIC"},
                  "expression": {"type": "CEL", "value": "true"},
                  "exemptions": %s
                }
                """.formatted(exemptions));

        // then
        assertThat(response)
                .hasStatusCode(400)
                .hasBody("{\"error\": \"" + expectedError + "\"}");
        assertThat(repository.getActiveConfig().governance().rules()).isEmpty();
    }

    static Stream<Arguments> invalidExemptions() {
        var ok = "{\"type\": \"CEL\", \"value\": \"true\"}";
        return Stream.of(
                Arguments.of("null exemption", "[null]",
                        "governance rule 'r': exemption must not be null"),
                Arguments.of("blank exemption name",
                        "[{\"name\": \" \", \"expression\": " + ok + "}]",
                        "governance rule 'r': exemption name must not be blank"),
                Arguments.of("duplicate exemption name",
                        "[{\"name\": \"e\", \"expression\": " + ok + "}, {\"name\": \"e\", \"expression\": " + ok + "}]",
                        "governance rule 'r': duplicate exemption name 'e'"),
                Arguments.of("missing exemption expression",
                        "[{\"name\": \"e\"}]",
                        "governance rule 'r': exemption 'e': expression must not be null"),
                Arguments.of("unsupported exemption expression type",
                        "[{\"name\": \"e\", \"expression\": {\"type\": \"SPEL\", \"value\": \"true\"}}]",
                        "governance rule 'r': unsupported exemption 'e' expression.type 'SPEL'"));
    }

    @Test
    void rejectsNonCompilingExemptionExpression() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", rulePath("r"), """
                {
                  "errorMessage": "msg",
                  "selector": {"resourceType": "TOPIC"},
                  "expression": {"type": "CEL", "value": "true"},
                  "exemptions": [{"name": "e", "expression": {"type": "CEL", "value": "principal =="}}]
                }
                """);

        // then
        assertThat(response).hasStatusCode(400);
        assertThatJson(response.body()).inPath("error").isString()
                .startsWith("governance rule 'r': exemption 'e' expression: Invalid CEL expression 'principal =='");
        assertThat(repository.getActiveConfig().governance().rules()).isEmpty();
    }

    @Test
    void replacesExistingRuleWithSameName() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty().upsertGovernanceRule(new GovernanceRuleConfig(
                "min-replication",
                "replication factor must be at least 2",
                null,
                Selector.topic(),
                Expression.cel("topic.replicationFactor >= 2"))));
        startServer();

        // when
        var response = send("PUT", rulePath("min-replication"), """
                {
                  "name": "min-replication",
                  "errorMessage": "replication factor must be at least 3",
                  "selector": {"resourceType": "TOPIC"},
                  "expression": {"type": "CEL", "value": "topic.replicationFactor >= 3"}
                }
                """);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        var rules = repository.getActiveConfig().governance().rules();
        assertThat(rules).hasSize(1);
        assertThat(rules.get("min-replication").expression()).isEqualTo(Expression.cel("topic.replicationFactor >= 3"));
        assertThat(rules.get("min-replication").errorMessage()).isEqualTo("replication factor must be at least 3");
    }

    @Test
    void rejectsBodyNameNotMatchingPath() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", rulePath("a"), """
                {
                  "name": "b",
                  "errorMessage": "msg",
                  "selector": {"resourceType": "TOPIC"},
                  "expression": {"type": "CEL", "value": "true"}
                }
                """);

        // then
        assertThat(response)
                .hasStatusCode(400)
                .hasBody("""
                        {"error": "governance rule 'a': body name 'b' does not match the path"}
                        """);
        assertThat(repository.getActiveConfig().governance().rules()).isEmpty();
    }

    @Test
    void rejectsUnparseableBody() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", rulePath("r"), "not json");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().governance().rules()).isEmpty();
    }

    @Test
    void rejectsNullBody() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", rulePath("r"), "null");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThatJson(response.body()).isEqualTo("""
                {"error": "governance rule: body must not be empty"}
                """);
        assertThat(repository.getActiveConfig().governance().rules()).isEmpty();
    }

    @Test
    void rejectsNullResourceType() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", rulePath("r"), """
                {
                  "name": "r",
                  "errorMessage": "msg",
                  "selector": {"resourceType": null},
                  "expression": {"type": "CEL", "value": "true"}
                }
                """);

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThatJson(response.body()).isEqualTo("""
                {"error": "governance rule 'r': selector.resourceType must not be blank"}
                """);
        assertThat(repository.getActiveConfig().governance().rules()).isEmpty();
    }

    @Test
    void rejectsEmptyResourceType() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", rulePath("r"), """
                {
                  "name": "r",
                  "errorMessage": "msg",
                  "selector": {"resourceType": ""},
                  "expression": {"type": "CEL", "value": "true"}
                }
                """);

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThatJson(response.body()).isEqualTo("""
                {"error": "governance rule 'r': selector.resourceType must not be blank"}
                """);
        assertThat(repository.getActiveConfig().governance().rules()).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidRules")
    void rejectsInvalidRule(String description, String body, String expectedError) throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", rulePath("r"), body);

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThatJson(response.body()).isEqualTo("{\"error\": \"" + expectedError + "\"}");
        assertThat(repository.getActiveConfig().governance().rules()).isEmpty();
    }

    static Stream<Arguments> invalidRules() {
        return Stream.of(
                Arguments.of("blank body name",
                        rule("  ", "msg", "{\"resourceType\": \"TOPIC\"}", "{\"type\": \"CEL\", \"value\": \"true\"}"),
                        "governance rule 'r': body name '  ' does not match the path"),
                Arguments.of("blank errorMessage",
                        rule("r", "", "{\"resourceType\": \"TOPIC\"}", "{\"type\": \"CEL\", \"value\": \"true\"}"),
                        "governance rule 'r': errorMessage must not be blank"),
                Arguments.of("missing selector",
                        rule("r", "msg", "null", "{\"type\": \"CEL\", \"value\": \"true\"}"),
                        "governance rule 'r': selector must not be null"),
                Arguments.of("missing expression and subRules",
                        rule("r", "msg", "{\"resourceType\": \"TOPIC\"}", "null"),
                        "governance rule 'r': subRules must not be empty"),
                Arguments.of("unsupported resourceType",
                        rule("r", "msg", "{\"resourceType\": \"CLUSTER\"}", "{\"type\": \"CEL\", \"value\": \"true\"}"),
                        "governance rule 'r': unsupported selector.resourceType 'CLUSTER'"),
                Arguments.of("unsupported scope",
                        rule("r", "msg", "{\"resourceType\": \"TOPIC\", \"scope\": \"SOME\"}", "{\"type\": \"CEL\", \"value\": \"true\"}"),
                        "governance rule 'r': unsupported selector.scope 'SOME'"),
                Arguments.of("scope on a non-topic rule",
                        rule("r", "msg", "{\"resourceType\": \"GROUP\", \"scope\": \"PHYSICAL\"}", "{\"type\": \"CEL\", \"value\": \"true\"}"),
                        "governance rule 'r': selector.scope 'PHYSICAL' only applies to TOPIC rules"),
                Arguments.of("unsupported expression type",
                        rule("r", "msg", "{\"resourceType\": \"TOPIC\"}", "{\"type\": \"SPEL\", \"value\": \"true\"}"),
                        "governance rule 'r': unsupported expression.type 'SPEL'"),
                Arguments.of("blank expression value",
                        rule("r", "msg", "{\"resourceType\": \"TOPIC\"}", "{\"type\": \"CEL\", \"value\": \" \"}"),
                        "governance rule 'r': expression.value must not be blank"));
    }

    @Test
    void rejectsNonCompilingExpression() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", rulePath("r"), rule("r", "msg",
                "{\"resourceType\": \"TOPIC\"}", "{\"type\": \"CEL\", \"value\": \"topic.partitions >=\"}"));

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThatJson(response.body()).inPath("error").isString()
                .startsWith("governance rule 'r': expression: Invalid CEL expression 'topic.partitions >='");
        assertThat(repository.getActiveConfig().governance().rules()).isEmpty();
    }

    @Test
    void rejectsNonCompilingSelectorExpression() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", rulePath("r"), rule("r", "msg",
                "{\"resourceType\": \"TOPIC\", \"expression\": {\"type\": \"CEL\", \"value\": \"topic.name ==\"}}",
                "{\"type\": \"CEL\", \"value\": \"true\"}"));

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThatJson(response.body()).inPath("error").isString()
                .startsWith("governance rule 'r': selector.expression: Invalid CEL expression 'topic.name =='");
        assertThat(repository.getActiveConfig().governance().rules()).isEmpty();
    }

    @Test
    void addsRuleWithSubRulesAndPersistsTree() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", rulePath("topic-naming"), """
                {
                  "errorMessage": "Topic names must follow the convention.",
                  "description": "One sub-rule per accepted form.",
                  "selector": {"resourceType": "TOPIC", "scope": "BOTH", "operations": ["CREATE"]},
                  "match": "ANY",
                  "subRules": [
                    {"kind": "check", "name": "model", "expression": {"type": "CEL", "value": "topic.name.startsWith('model.')"}},
                    {
                      "kind": "group",
                      "name": "app",
                      "errorMessage": "App topics are app.<service>[.<subject>].",
                      "match": "ANY",
                      "checks": [
                        {"kind": "check", "name": "standard", "expression": {"type": "CEL", "value": "topic.name.startsWith('app.')"}},
                        {"kind": "check", "name": "changelog", "expression": {"type": "CEL", "value": "topic.name.endsWith('-changelog')"}}
                      ]
                    }
                  ]
                }
                """);

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).inPath("match").isEqualTo("ANY");
        assertThatJson(response.body()).inPath("subRules[1].checks[0].name").isEqualTo("standard");
        assertThatJson(response.body()).inPath("subRules[1].kind").isEqualTo("group");
        var rule = repository.getActiveConfig().governance().rules().get("topic-naming");
        assertThat(rule.subRules())
                .withFailMessage(() -> "Sub-rule tree of 'topic-naming' was not persisted as sent")
                .containsExactly(
                        GovernanceRuleConfig.SubRule.check("model", Expression.cel("topic.name.startsWith('model.')")),
                        new GovernanceRuleConfig.SubRule.Group("app", "App topics are app.<service>[.<subject>].",
                                GovernanceRuleConfig.Match.ANY, List.of(
                                GovernanceRuleConfig.SubRule.check("standard", Expression.cel("topic.name.startsWith('app.')")),
                                GovernanceRuleConfig.SubRule.check("changelog", Expression.cel("topic.name.endsWith('-changelog')")))));
        assertThat(rule.expression())
                .isEqualTo(Expression.cel(
                        "(topic.name.startsWith('model.')) || ((topic.name.startsWith('app.')) || (topic.name.endsWith('-changelog')))"));
    }

    @Test
    void rejectsGroupInsideGroup() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", rulePath("r"), """
                {
                  "errorMessage": "msg",
                  "selector": {"resourceType": "TOPIC"},
                  "match": "ALL",
                  "subRules": [
                    {
                      "kind": "group",
                      "name": "outer",
                      "match": "ANY",
                      "checks": [
                        {"kind": "group", "name": "inner", "match": "ALL",
                         "checks": [{"kind": "check", "name": "c", "expression": {"type": "CEL", "value": "true"}}]}
                      ]
                    }
                  ]
                }
                """);

        // then
        assertThat(response)
                .hasStatusCode(400)
                .hasBody("{\"error\": \"governance rule 'r': subRules[outer].checks[inner]: a group holds checks only, not 'group'\"}");
        assertThat(repository.getActiveConfig().governance().rules()).isEmpty();
    }

    @Test
    void compilesExpressionsAgainstTheRuleResourceType() throws Exception {
        // given
        startServer();

        // when
        var groupRule = send("PUT", rulePath("group-naming"), """
                {
                  "errorMessage": "msg",
                  "selector": {"resourceType": "GROUP"},
                  "match": "ALL",
                  "subRules": [{"kind": "check", "name": "app", "expression": {"type": "CEL", "value": "group.id.startsWith('app.')"}}]
                }
                """);
        var topicRuleReadingGroup = send("PUT", rulePath("wrong"), """
                {
                  "errorMessage": "msg",
                  "selector": {"resourceType": "TOPIC"},
                  "match": "ALL",
                  "subRules": [{"kind": "check", "name": "app", "expression": {"type": "CEL", "value": "group.id.startsWith('app.')"}}]
                }
                """);

        // then
        assertThat(groupRule).hasStatusCode(200);
        assertThat(topicRuleReadingGroup).hasStatusCode(400);
        assertThatJson(topicRuleReadingGroup.body()).inPath("error").isString()
                .startsWith("governance rule 'wrong': subRules[app].expression: Invalid CEL expression");
    }

    private static String rulePath(String name) {
        return "/governance/rules/" + name;
    }

    private static String rule(String name, String errorMessage, String selector, String expression) {
        return """
                {
                  "name": "%s",
                  "errorMessage": "%s",
                  "selector": %s,
                  "expression": %s
                }
                """.formatted(name, errorMessage, selector, expression);
    }
}
