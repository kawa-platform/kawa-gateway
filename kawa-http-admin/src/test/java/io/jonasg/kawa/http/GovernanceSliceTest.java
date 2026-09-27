package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceExemptionConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static io.jonasg.kawa.http.KawaAssertions.assertThat;

/// Slice tests for the `/governance` admin surface: real HTTP requests through a booted
/// [AdminHttpServer], asserting the JSON wire format the admin UI consumes.
class GovernanceSliceTest extends AdminHttpSliceTestBase {

    @Test
    void listsConfiguredGovernance() throws Exception {
        // given
        var governanceConfig = new GovernanceConfig(
                Map.of("min-replication",
                        new GovernanceRuleConfig("replication factor must be at least 3",
                                "topic.replicationFactor >= 3")),
                Map.of("ops", new GovernanceExemptionConfig(".*", ".*-changelog")));
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty().updateGovernance(governanceConfig));
        startServer();

        // when
        var response = send("GET", "/governance", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                {
                  "topicRules": {
                    "min-replication": {
                      "message": "replication factor must be at least 3",
                      "expression": "topic.replicationFactor >= 3"
                    }
                  },
                  "exemptions": {
                    "ops": {
                      "principal": ".*",
                      "topicPattern": ".*-changelog"
                    }
                  }
                }
                """);
    }

    @Test
    void listsEmptyGovernanceWhenNoSnapshotApplied() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(null);
        startServer();

        // when
        var response = send("GET", "/governance", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                {"topicRules":{},"exemptions":{}}
                """);
    }

    @Test
    void replacesGovernanceAndPersistsSnapshot() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/governance",
                """
                        {
                          "topicRules": {
                            "min-replication": {
                              "message": "replication factor must be at least 3",
                              "expression": "topic.replicationFactor >= 3"
                            }
                          },
                          "exemptions": {
                            "ops": {
                              "principal": ".*",
                              "topicPattern": ".*-changelog"
                            }
                          }
                        }
                        """);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().governance().topicRules()).containsKey("min-replication");
        assertThat(repository.getActiveConfig().governance().exemptions()).containsKey("ops");
        assertThat(repository.updateCalls()).isEqualTo(1);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(0);
    }

    @Test
    void replacesGovernanceWithAppliedConsistencyWaitsForApplyMode() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/governance?consistency=applied",
                """
                        {
                          "topicRules": {
                            "min-replication": {
                              "message": "replication factor must be at least 3",
                              "expression": "topic.replicationFactor >= 3"
                            }
                          },
                          "exemptions": {
                            "ops": {
                              "principal": ".*",
                              "topicPattern": ".*-changelog"
                            }
                          }
                        }
                        """);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.updateCalls()).isEqualTo(0);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(1);
    }

    @Test
    void rejectsInvalidConsistencyForGovernanceWrite() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/governance?consistency=eventual",
                """
                        {
                          "topicRules": {
                            "min-replication": {
                              "message": "replication factor must be at least 3",
                              "expression": "topic.replicationFactor >= 3"
                            }
                          }
                        }
                        """);

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("invalid consistency 'eventual'");
        assertThat(repository.updateCalls()).isEqualTo(0);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(0);
    }

    @Test
    void rejectsInvalidRuleExpression() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/governance",
                """
                        {
                          "topicRules": {
                            "broken": {
                              "message": "must be valid",
                              "expression": "topic.replicationFactor >="
                            }
                          }
                        }
                        """);

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().governance().topicRules()).isEmpty();
    }

    @Test
    void rejectsInvalidGovernanceBody() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/governance", "not json");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().governance().topicRules()).isEmpty();
    }

    @Test
    void rejectsMalformedExemptionRegexWithoutLeakingInternals() throws Exception {
        // given
        startServer();

        // when
        var putResp = send("PUT", "/governance",
                "{\"exemptions\":{\"e1\":{\"principal\":\"[unclosed\",\"topicPattern\":\".*\"}}}");

        // then
        assertThat(putResp.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().governance().topicRules()).isEmpty();
        assertThat(putResp.body())
                .contains("exemption 'e1'")
                .doesNotContain("PatternSyntaxException")
                .doesNotContain("near index")
                .doesNotContain("[unclosed");
    }

    @Test
    void listsGovernanceWithExactlyTopicRulesAndExemptions() throws Exception {
        // given
        var governanceConfig = new GovernanceConfig(
                Map.of("min-replication",
                        new GovernanceRuleConfig("replication factor must be at least 3",
                                "topic.replicationFactor >= 3")),
                Map.of("ops", new GovernanceExemptionConfig(".*", ".*-changelog")));
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty().updateGovernance(governanceConfig));
        startServer();

        // when
        var governanceResp = send("GET", "/governance", null);

        // then
        assertThat(governanceResp.statusCode()).isEqualTo(200);
        assertThat(governanceResp).containsExactlyTopLevelPropertyNames("topicRules", "exemptions");
    }

    @Test
    void putResponseCarriesTheSameNestedShapeAsTheRequest() throws Exception {
        // given
        startServer();

        // when
        var putResp = send("PUT", "/governance",
                """
                        {
                          "topicRules": {
                            "min-replication": {
                              "message": "replication factor must be at least 3",
                              "expression": "topic.replicationFactor >= 3"
                            }
                          },
                          "exemptions": {
                            "ops": {
                              "principal": ".*",
                              "topicPattern": ".*-changelog"
                            }
                          }
                        }
                        """);

        // then
        assertThat(putResp.statusCode()).isEqualTo(200);
        assertThat(putResp).containsExactlyTopLevelPropertyNames("topicRules", "exemptions");
        // and - the nested rule and exemption key sets are frozen too, which the top-level
        // key assertion above cannot see
        assertThatJson(putResp.body()).isEqualTo("""
                {
                  "topicRules": {
                    "min-replication": {
                      "message": "replication factor must be at least 3",
                      "expression": "topic.replicationFactor >= 3"
                    }
                  },
                  "exemptions": {
                    "ops": {
                      "principal": ".*",
                      "topicPattern": ".*-changelog"
                    }
                  }
                }
                """);
    }
}
