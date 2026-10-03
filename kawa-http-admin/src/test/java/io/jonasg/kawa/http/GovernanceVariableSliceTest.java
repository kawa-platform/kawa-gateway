package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GovernanceVariableConfig;
import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for [PutGovernanceVariableHandler], [GetGovernanceVariablesHandler] and
/// [DeleteGovernanceVariableHandler], and for rules reading variables.
class GovernanceVariableSliceTest extends AdminHttpSliceTestBase {

    private static final String TIERS = """
            {"type": "list<int>", "value": "[1, 4, 6, 12]", "note": "single, low, medium, high."}
            """;

    private static final String TIER_RULE = """
            {
              "errorMessage": "partitions must match a tier",
              "selector": {"resourceType": "TOPIC", "scope": "PHYSICAL"},
              "match": "ALL",
              "subRules": [{"kind": "check", "name": "tier", "expression": {"type": "CEL", "value": "topic.partitions in partitionTiers"}}]
            }
            """;

    @Test
    void addsVariableAndPersistsSnapshot() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/governance/variables/partitionTiers", TIERS);

        // then
        assertThat(response)
                .hasStatusCode(200)
                .hasBody("""
                        {
                          "name": "partitionTiers",
                          "type": "list<int>",
                          "value": "[1, 4, 6, 12]",
                          "note": "single, low, medium, high.",
                          "samples": {},
                          "notes": {},
                          "extraExamples": {}
                        }
                        """);
        assertThat(repository.getActiveConfig().governance().variables().get("partitionTiers"))
                .withFailMessage(() -> "Variable 'partitionTiers' was not persisted")
                .isEqualTo(new GovernanceVariableConfig(
                        "partitionTiers", GovernanceVariableConfig.Type.LIST_INT, "[1, 4, 6, 12]", "single, low, medium, high."));
    }

    @Test
    void rejectsValueOfAnotherType() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/governance/variables/partitionTiers", """
                {"type": "list<int>", "value": "[\\"low\\"]"}
                """);

        // then
        assertThat(response)
                .hasStatusCode(400)
                .hasBody("{\"error\": \"governance variable 'partitionTiers': value is not a list<int>\"}");
    }

    @Test
    void rejectsUnsupportedType() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/governance/variables/x", """
                {"type": "map", "value": "{}"}
                """);

        // then
        assertThat(response)
                .hasStatusCode(400)
                .hasBody("{\"error\": \"governance variable 'x': unsupported type 'map'\"}");
    }

    @Test
    void ruleReadingVariableIsAcceptedOnlyOnceTheVariableExists() throws Exception {
        // given
        startServer();

        // when
        var before = send("PUT", "/governance/rules/partition-tier", TIER_RULE);
        send("PUT", "/governance/variables/partitionTiers", TIERS);
        var after = send("PUT", "/governance/rules/partition-tier", TIER_RULE);

        // then
        assertThat(before).hasStatusCode(400);
        assertThatJson(before.body()).inPath("error").isString()
                .startsWith("governance rule 'partition-tier': subRules[tier].expression: Invalid CEL expression");
        assertThat(after).hasStatusCode(200);
    }

    @Test
    void refusesToDeleteOrRetypeVariableARuleStillReads() throws Exception {
        // given
        startServer();
        send("PUT", "/governance/variables/partitionTiers", TIERS);
        send("PUT", "/governance/rules/partition-tier", TIER_RULE);

        // when
        var delete = send("DELETE", "/governance/variables/partitionTiers");
        var retype = send("PUT", "/governance/variables/partitionTiers", """
                {"type": "string", "value": "\\"1\\""}
                """);

        // then
        assertThat(delete).hasStatusCode(400);
        assertThatJson(delete.body()).inPath("error").isString().contains("partition-tier");
        assertThat(retype).hasStatusCode(400);
        assertThat(repository.getActiveConfig().governance().variables())
                .withFailMessage(() -> "Variable 'partitionTiers' changed although a rule reads it")
                .containsKey("partitionTiers");
    }

    @Test
    void listsAndDeletesUnusedVariable() throws Exception {
        // given
        startServer();
        send("PUT", "/governance/variables/partitionTiers", TIERS);

        // when
        var list = send("GET", "/governance/variables");
        var delete = send("DELETE", "/governance/variables/partitionTiers");
        var deleteAgain = send("DELETE", "/governance/variables/partitionTiers");

        // then
        assertThatJson(list.body()).isArray().hasSize(1);
        assertThat(delete).hasStatusCode(204);
        assertThat(deleteAgain).hasStatusCode(404);
    }
}
