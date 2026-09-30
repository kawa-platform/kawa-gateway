package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig.Expression;
import io.jonasg.kawa.config.GovernanceRuleConfig.Selector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
                  "selector": {"resourceType": "TOPIC", "expression": {"type": "CEL", "value": "true"}},
                  "expression": {"type": "CEL", "value": "topic.name.startsWith('event.')"},
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
                  "selector": {"resourceType": "TOPIC", "expression": null},
                  "expression": {"type": "CEL", "value": "topic.replicationFactor >= 3"},
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
                            "expression": null
                          },
                          "expression": {
                            "type": "CEL",
                            "value": "topic.partitions >= 2"
                          },
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
                Arguments.of("missing expression",
                        rule("r", "msg", "{\"resourceType\": \"TOPIC\"}", "null"),
                        "governance rule 'r': expression must not be null"),
                Arguments.of("unsupported resourceType",
                        rule("r", "msg", "{\"resourceType\": \"GROUP\"}", "{\"type\": \"CEL\", \"value\": \"true\"}"),
                        "governance rule 'r': unsupported selector.resourceType 'GROUP'"),
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
