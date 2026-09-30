package io.jonasg.kawa.http;

import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;

/// Slice tests for [GetGovernanceRulesHandler]: real HTTP requests through a booted [AdminHttpServer],
/// asserting the JSON wire format the admin UI consumes.
class GetGovernanceRulesSliceTest extends AdminHttpSliceTestBase {

    @Test
    void returnsRuleWithItsExemptions() throws Exception {
        // given
        startServer();
        var putRuleResp = send("PUT", "/governance/rules/min-partitions", """
                {
                  "errorMessage": "partitions must be at least 2",
                  "description": "All topics must have at least 2 partitions.",
                  "selector": {"resourceType": "TOPIC"},
                  "expression": {"type": "CEL", "value": "topic.partitions >= 2"},
                  "exemptions": [
                    {
                      "name": "streams-internal",
                      "description": "Kafka Streams manages its own changelog topics.",
                      "expression": {
                        "type": "CEL",
                        "value": "principal.startsWith('streams-') && topic.name.endsWith('-changelog')"
                      }
                    }
                  ]
                }
                """);
        assertThat(putRuleResp).hasStatusCode(200);

        // when
        var getRuleResp = send("GET", "/governance/rules/min-partitions");

        // then
        assertThat(getRuleResp)
                .hasStatusCode(200)
                .hasBody("""
                        {
                          "name": "min-partitions",
                          "errorMessage": "partitions must be at least 2",
                          "description": "All topics must have at least 2 partitions.",
                          "selector": {"resourceType": "TOPIC", "expression": null},
                          "expression": {"type": "CEL", "value": "topic.partitions >= 2"},
                          "exemptions": [
                            {
                              "name": "streams-internal",
                              "description": "Kafka Streams manages its own changelog topics.",
                              "expression": {
                                "type": "CEL",
                                "value": "principal.startsWith('streams-') && topic.name.endsWith('-changelog')"
                              }
                            }
                          ]
                        }
                        """);
    }
}
