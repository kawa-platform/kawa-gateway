package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig.Expression;
import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for the global exemption handlers ([PutGovernanceExemptionHandler],
/// [GetGovernanceExemptionsHandler], [GetGovernanceExemptionHandler],
/// [DeleteGovernanceExemptionHandler]) and [DeleteGovernanceRuleHandler].
class GovernanceExemptionSliceTest extends AdminHttpSliceTestBase {

    @Test
    void addsGlobalExemptionAndPersistsSnapshot() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/governance/exemptions/cargo-scratch", """
                {
                  "description": "Scratch space for the cargo team.",
                  "expression": {"type": "CEL", "value": "principal == 'User:app-cargo' && topic.name.startsWith('scratch.')"}
                }
                """);

        // then
        assertThat(response)
                .hasStatusCode(200)
                .hasBody("""
                        {
                          "name": "cargo-scratch",
                          "description": "Scratch space for the cargo team.",
                          "expression": {"type": "CEL", "value": "principal == 'User:app-cargo' && topic.name.startsWith('scratch.')"}
                        }
                        """);
        assertThat(repository.getActiveConfig().governance().exemptions().get("cargo-scratch"))
                .withFailMessage(() -> "Global exemption 'cargo-scratch' was not persisted")
                .isEqualTo(new GovernanceRuleConfig.Exemption(
                        "cargo-scratch",
                        "Scratch space for the cargo team.",
                        Expression.cel("principal == 'User:app-cargo' && topic.name.startsWith('scratch.')")));
    }

    @Test
    void acceptsExpressionReadingAnyResourceVariable() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/governance/exemptions/connect", """
                {"expression": {"type": "CEL", "value": "group.id.startsWith('connect-') || topic.name.startsWith('connect-')"}}
                """);

        // then
        assertThat(response).hasStatusCode(200);
    }

    @Test
    void rejectsNonCompilingExpression() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/governance/exemptions/broken", """
                {"expression": {"type": "CEL", "value": "principal =="}}
                """);

        // then
        assertThat(response).hasStatusCode(400);
        assertThatJson(response.body()).inPath("error").isString()
                .startsWith("governance exemption 'broken': expression: Invalid CEL expression 'principal =='");
        assertThat(repository.getActiveConfig().governance().exemptions()).isEmpty();
    }

    @Test
    void listsReadsAndDeletesGlobalExemptions() throws Exception {
        // given
        startServer();
        send("PUT", "/governance/exemptions/a", """
                {"expression": {"type": "CEL", "value": "principal == 'User:a'"}}
                """);

        // when
        var list = send("GET", "/governance/exemptions");
        var one = send("GET", "/governance/exemptions/a");
        var delete = send("DELETE", "/governance/exemptions/a");
        var missing = send("GET", "/governance/exemptions/a");

        // then
        assertThat(list).hasStatusCode(200);
        assertThatJson(list.body()).isArray().hasSize(1);
        assertThat(one).hasStatusCode(200);
        assertThat(delete).hasStatusCode(204);
        assertThat(missing).hasStatusCode(404);
        assertThat(repository.getActiveConfig().governance().exemptions())
                .withFailMessage(() -> "Global exemption 'a' was not removed")
                .isEmpty();
    }

    @Test
    void deletesGovernanceRule() throws Exception {
        // given
        startServer();
        send("PUT", "/governance/rules/r", """
                {"errorMessage": "msg", "selector": {"resourceType": "TOPIC"}, "expression": {"type": "CEL", "value": "true"}}
                """);

        // when
        var delete = send("DELETE", "/governance/rules/r");
        var deleteAgain = send("DELETE", "/governance/rules/r");

        // then
        assertThat(delete).hasStatusCode(204);
        assertThat(deleteAgain).hasStatusCode(404);
        assertThat(repository.getActiveConfig().governance().rules())
                .withFailMessage(() -> "Governance rule 'r' was not removed")
                .isEmpty();
    }
}
