package io.jonasg.kawa.http;

import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;

/// Slice tests for [GetGovernanceHandler]: real HTTP requests through a booted [AdminHttpServer],
/// asserting the JSON wire format the admin UI consumes.
class GetGovernanceSliceTest extends AdminHttpSliceTestBase {

    @Test
    void getGovernanceRules() throws Exception {
        // given
        startServer();
        // and
        var response = send("PUT", "/governance/rules/min-replication?consistency=applied",
                """
                        {
                          "name": "min-replication",
                          "errorMessage": "replication factor must be at least 3",
                          "selector": {"resourceType": "TOPIC"},
                          "expression": {"type": "CEL", "value": "topic.replicationFactor >= 3"}
                        }
                        """);
        assertThat(response).hasStatusCode(200);

        // when
        response = send("GET", "/governance/rules");

        // then
        assertThat(response)
                .hasStatusCode(200)
                .hasBody("""
                        {
                          "rules": [
                            {
                              "name": "min-replication",
                              "errorMessage": "replication factor must be at least 3",
                              "description": null,
                              "selector": {
                                "resourceType": "TOPIC",
                                "expression": null
                              },
                              "expression": {
                                "type": "CEL",
                                "value": "topic.replicationFactor >= 3"
                              }
                            }
                          ],
                          "exemptions": []
                        }""");
    }
}
