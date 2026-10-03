package io.jonasg.kawa.http;

import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for [PostGovernanceDryRunHandler].
class GovernanceDryRunSliceTest extends AdminHttpSliceTestBase {

    private static final String NAMING_RULE = """
            {
              "name": "naming",
              "errorMessage": "Topic names must follow the convention.",
              "selector": {"resourceType": "TOPIC"},
              "match": "ANY",
              "subRules": [
                {"kind": "check", "name": "model", "expression": {"type": "CEL", "value": "topic.name.startsWith('model.')"}},
                {"kind": "check", "name": "app", "expression": {"type": "CEL", "value": "topic.name.startsWith('app.')"}}
              ]
            }
            """;

    @Test
    void tracesAnUnsavedRuleCheckByCheck() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/governance/dry-run", """
                {
                  "resourceType": "TOPIC",
                  "resource": {"name": "app.orders", "partitions": 3, "replicationFactor": 3, "configs": {}},
                  "principal": "User:alice",
                  "rule": %s
                }
                """.formatted(NAMING_RULE));

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).inPath("allowed").isEqualTo(true);
        assertThatJson(response.body()).inPath("rules[0].subRules[*].outcome").isArray().containsExactly("FAIL", "PASS");
    }

    @Test
    void explainsARefusal() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/governance/dry-run", """
                {"resourceType": "TOPIC", "resource": {"name": "misc.orders"}, "rule": %s}
                """.formatted(NAMING_RULE));

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).inPath("allowed").isEqualTo(false);
        assertThatJson(response.body()).inPath("rules[0].outcome").isEqualTo("FAIL");
        assertThatJson(response.body()).inPath("rules[0].message").isEqualTo("Topic names must follow the convention.");
    }

    @Test
    void rejectsMissingResourceName() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/governance/dry-run", """
                {"resourceType": "GROUP", "resource": {}}
                """);

        // then
        assertThat(response)
                .hasStatusCode(400)
                .hasBody("{\"error\": \"governance dry-run: resource.id must not be blank\"}");
    }
}
