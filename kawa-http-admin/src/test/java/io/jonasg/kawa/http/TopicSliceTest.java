package io.jonasg.kawa.http;

import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceExemptionConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.HeaderContainsFilterConfig;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.HeaderMatchesFilterConfig;
import io.jonasg.kawa.config.HeaderStartsWithFilterConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.governance.TopicSpec;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static net.javacrumbs.jsonunit.core.Option.IGNORING_ARRAY_ORDER;
import static org.assertj.core.api.Assertions.assertThat;

/// Slice tests for the `/topics` admin surface: real HTTP requests through a booted
/// [AdminHttpServer], asserting the JSON wire format the admin UI consumes.
class TopicSliceTest extends AdminHttpSliceTestBase {

    @Test
    void servesVirtualAndPhysicalEntriesOverHttp() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2"),
                "customers", new VirtualTopicConfig("crm.customers",
                        new HeaderEqualsFilterConfig("tenant", "acme"), true)));
        cache = cacheWith(
                topic("orders-v2", 3, 2),
                topic("crm.customers", 2, 3),
                topic("raw-events", 1, 1));
        startServer();

        // when
        var topicsResp = send("GET", "/topics", null);

        // then
        assertThat(topicsResp.statusCode()).isEqualTo(200);
        assertThatJson(topicsResp.body())
                .when(IGNORING_ARRAY_ORDER)
                .isEqualTo("""
                        [
                          {
                            "type": "virtual",
                            "name": "orders",
                            "partitions": 3,
                            "replicationFactor": 2,
                            "filter": null,
                            "physicalTopic": "orders-v2",
                            "valueFormat": null
                          },
                          {
                            "type": "physical",
                            "name": "orders-v2",
                            "partitions": 3,
                            "replicationFactor": 2,
                            "filter": null,
                            "physicalTopic": null,
                            "valueFormat": null
                          },
                          {
                            "type": "virtual",
                            "name": "customers",
                            "partitions": 2,
                            "replicationFactor": 3,
                            "filter": {
                              "kind": "header",
                              "expression": "tenant=acme"
                            },
                            "physicalTopic": "crm.customers",
                            "valueFormat": null
                          },
                          {
                            "type": "physical",
                            "name": "crm.customers",
                            "partitions": 2,
                            "replicationFactor": 3,
                            "filter": null,
                            "physicalTopic": null,
                            "valueFormat": null
                          },
                          {
                            "type": "physical",
                            "name": "raw-events",
                            "partitions": 1,
                            "replicationFactor": 1,
                            "filter": null,
                            "physicalTopic": null,
                            "valueFormat": null
                          }
                        ]
                        """);
    }

    @Test
    void returnsEmptyListWhenNoTopics() throws Exception {
        // given
        startServer();

        // when
        var response = send("GET", "/topics", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("[]");
    }

    @Test
    void describesCelFilterOnVirtualEntry() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of(
                "audit", new VirtualTopicConfig("audit-v1",
                        new CelFilterConfig("headers.tenant == \"acme\""), false)));
        cache = cacheWith(topic("audit-v1", 1, 1));
        startServer();

        // when
        var response = send("GET", "/topics", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                [{
                  "type": "virtual",
                  "name": "audit",
                  "partitions": 1,
                  "replicationFactor": 1,
                  "filter": {
                    "kind": "cel",
                    "expression": "headers.tenant == \\\"acme\\\""
                  },
                  "physicalTopic": "audit-v1",
                  "valueFormat": null
                },
                {
                  "type": "physical",
                  "name": "audit-v1",
                  "partitions": 1,
                  "replicationFactor": 1,
                  "filter": null,
                  "physicalTopic": null,
                  "valueFormat": null
                }]
                """);
    }

    @Test
    void describesHeaderFilterDetailsForVirtualTopics() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of(
                "contains", new VirtualTopicConfig("contains-v1",
                        new HeaderContainsFilterConfig("tenant", "acm"), false),
                "startsWith", new VirtualTopicConfig("starts-v1",
                        new HeaderStartsWithFilterConfig("tenant", "ac"), false),
                "matches", new VirtualTopicConfig("matches-v1",
                        new HeaderMatchesFilterConfig("tenant", "eu.*"), false)));
        cache = cacheWith(topic("contains-v1", 1, 1), topic("starts-v1", 1, 1), topic("matches-v1", 1, 1));
        startServer();

        // when
        var topicsResp = send("GET", "/topics", null);

        // then
        assertThat(topicsResp.statusCode()).isEqualTo(200);
        assertThatJson(topicsResp.body())
                .when(IGNORING_ARRAY_ORDER)
                .isEqualTo("""
                        [
                          {
                            "type": "virtual",
                            "name": "contains",
                            "partitions": 1,
                            "replicationFactor": 1,
                            "filter": {
                              "kind": "headerContains",
                              "expression": "tenant contains acm"
                            },
                            "physicalTopic": "contains-v1",
                            "valueFormat": null
                          },
                          {
                            "type": "virtual",
                            "name": "startsWith",
                            "partitions": 1,
                            "replicationFactor": 1,
                            "filter": {
                              "kind": "headerStartsWith",
                              "expression": "tenant starts with ac"
                            },
                            "physicalTopic": "starts-v1",
                            "valueFormat": null
                          },
                          {
                            "type": "virtual",
                            "name": "matches",
                            "partitions": 1,
                            "replicationFactor": 1,
                            "filter": {
                              "kind": "headerMatches",
                              "expression": "tenant matches eu.*"
                            },
                            "physicalTopic": "matches-v1",
                            "valueFormat": null
                          },
                          {
                            "type": "physical",
                            "name": "contains-v1",
                            "partitions": 1,
                            "replicationFactor": 1,
                            "filter": null,
                            "physicalTopic": null,
                            "valueFormat": null
                          },
                          {
                            "type": "physical",
                            "name": "starts-v1",
                            "partitions": 1,
                            "replicationFactor": 1,
                            "filter": null,
                            "physicalTopic": null,
                            "valueFormat": null
                          },
                          {
                            "type": "physical",
                            "name": "matches-v1",
                            "partitions": 1,
                            "replicationFactor": 1,
                            "filter": null,
                            "physicalTopic": null,
                            "valueFormat": null
                          }
                        ]
                        """);
    }

    @Test
    void listsVirtualTopicWithoutPhysicalBacking() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2")));
        startServer();

        // when
        var response = send("GET", "/topics", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                [{
                  "type": "virtual",
                  "name": "orders",
                  "partitions": 0,
                  "replicationFactor": 0,
                  "filter": null,
                  "physicalTopic": "orders-v2",
                  "valueFormat": null
                }]
                """);
    }

    @Test
    void createsPhysicalTopicOnBroker() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/topics",
                """
                        {
                          "type": "physical",
                          "name": "orders",
                          "partitions": 3,
                          "replicationFactor": 3,
                          "configs": {
                            "cleanup.policy": "compact"
                          }
                        }
                        """);

        // then
        assertThat(response.statusCode()).isEqualTo(201);
        assertThatJson(response.body()).isEqualTo("""
                {
                  "name": "orders",
                  "partitions": 3,
                  "replicationFactor": 3,
                  "configs": {"cleanup.policy": "compact"}
                }
                """);
        assertThat(topicAdmin.created)
                .containsExactly(new TopicSpec("orders", 3, 3, Map.of("cleanup.policy", "compact")));
    }

    @Test
    void rejectsInvalidTopicBody() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/topics", "not json");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(topicAdmin.created).isEmpty();
    }

    @Test
    void rejectsUnknownTopicType() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/topics", "{\"type\":\"wormhole\",\"name\":\"orders\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(topicAdmin.created).isEmpty();
    }

    @Test
    void rejectsTopicWithoutName() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/topics", "{\"type\":\"physical\",\"partitions\":3}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(topicAdmin.created).isEmpty();
    }

    @Test
    void rejectsTopicViolatingGovernance() throws Exception {
        // given
        governance = new GovernancePolicy(new GovernanceConfig(
                Map.of("min-replication",
                        new GovernanceRuleConfig("replication factor must be at least 3",
                                "topic.replicationFactor >= 3")),
                Map.of()));
        startServer();

        // when
        var response = send("POST", "/topics",
                "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":1}");

        // then
        assertThat(response.statusCode()).isEqualTo(403);
        assertThat(response.body()).contains("replication factor must be at least 3");
        assertThat(topicAdmin.created).isEmpty();
    }

    @Test
    void acceptsExemptTopicAndCreatesOnBroker() throws Exception {
        // given
        governance = new GovernancePolicy(new GovernanceConfig(
                Map.of("min-replication",
                        new GovernanceRuleConfig("replication factor must be at least 3",
                                "topic.replicationFactor >= 3")),
                Map.of("ops", new GovernanceExemptionConfig("admin", ".*"))));
        startServer();

        // when
        var response = send("POST", "/topics",
                "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":1}");

        // then
        assertThat(response.statusCode()).isEqualTo(201);
        assertThat(topicAdmin.created).hasSize(1);
    }

    @Test
    void returnsConflictWhenTopicExists() throws Exception {
        // given
        topicAdmin.createError = new TopicExistsException("Topic 'orders' already exists.");
        startServer();

        // when
        var response = send("POST", "/topics",
                "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":3}");

        // then
        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(topicAdmin.created).isEmpty();
    }

    @Test
    void createsVirtualTopicConfig() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/topics", "{\"type\":\"virtual\",\"name\":\"orders\",\"topic\":\"orders-v2\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(201);
        assertThatJson(response.body()).inPath("topic").isEqualTo("orders-v2");
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"));
        assertThat(topicAdmin.created).isEmpty();
        assertThat(repository.updateCalls()).isEqualTo(1);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(0);
    }

    @Test
    void createsVirtualTopicWithJsonValueFormat() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/topics", """
                {
                  "type": "virtual",
                  "name": "paid-orders",
                  "topic": "orders",
                  "filter": {"type": "cel", "expression": "value.status == \\"PAID\\""},
                  "valueFormat": {"type": "json", "onDecodeError": "include"}
                }
                """);

        // then
        assertThat(response.statusCode()).isEqualTo(201);
        assertThat(repository.getActiveConfig().virtualTopics())
                .withFailMessage(() -> "Virtual topic was not persisted with its JSON value format")
                .containsEntry("paid-orders", new VirtualTopicConfig(
                        "orders",
                        new CelFilterConfig("value.status == \"PAID\""),
                        false,
                        new JsonFormatConfig(DecodeErrorPolicy.INCLUDE)));
        assertThatJson(response.body()).inPath("valueFormat").isEqualTo("""
                {"type": "json", "onDecodeError": "include"}
                """);
    }

    @Test
    void describesValueFormatOnVirtualEntry() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of(
                "paid-orders", new VirtualTopicConfig("orders",
                        new CelFilterConfig("value.status == \"PAID\""), false, new JsonFormatConfig())));
        cache = cacheWith(topic("orders", 1, 1));
        startServer();

        // when
        var response = send("GET", "/topics", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).inPath("[0].valueFormat").isEqualTo("""
                {"type": "json", "onDecodeError": "skip"}
                """);
    }

    @Test
    void createsVirtualTopicWithAppliedConsistencyWaitsForApplyMode() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/topics?consistency=applied", "{\"type\":\"virtual\",\"name\":\"orders\",\"topic\":\"orders-v2\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(201);
        assertThat(repository.updateCalls()).isEqualTo(0);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(1);
    }

    @Test
    void rejectsInvalidConsistencyForVirtualTopicCreate() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/topics?consistency=strong", "{\"type\":\"virtual\",\"name\":\"orders\",\"topic\":\"orders-v2\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("invalid consistency 'strong'");
        assertThat(repository.updateCalls()).isEqualTo(0);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(0);
    }

    @Test
    void rejectsVirtualTopicWithoutBacking() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/topics", "{\"type\":\"virtual\",\"name\":\"orders\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().virtualTopics()).isEmpty();
    }

    @Test
    void addsVirtualTopicAndPersistsSnapshot() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/topics/orders", "{\"type\":\"virtual\",\"topic\":\"raw-orders\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).inPath("topic").isEqualTo("raw-orders");
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("raw-orders"));
        assertThat(repository.updateCalls()).isEqualTo(1);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(0);
    }

    @Test
    void updatesVirtualTopicWithAppliedConsistencyWaitsForApplyMode() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/topics/orders?consistency=applied", "{\"type\":\"virtual\",\"topic\":\"raw-orders\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.updateCalls()).isEqualTo(0);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(1);
    }

    @Test
    void overwritesExistingVirtualTopic() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("raw-old")));
        startServer();

        // when
        var response = send("PUT", "/topics/orders", "{\"type\":\"virtual\",\"topic\":\"raw-new\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().virtualTopics()).hasSize(1);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("raw-new"));
    }

    @Test
    void rejectsInvalidUpdateBody() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/topics/orders", "not json");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().virtualTopics()).isEmpty();
    }

    @Test
    void rejectsUpdatingPhysicalTopic() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/topics/orders", "{\"type\":\"physical\",\"partitions\":5}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().virtualTopics()).isEmpty();
    }

    @Test
    void rejectsUpdatingVirtualTopicWithoutBacking() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/topics/orders", "{\"type\":\"virtual\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().virtualTopics()).isEmpty();
    }

    @Test
    void patchesVirtualTopicConfiguration() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v1")));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders",
                """
                        {
                          "topic": "orders-v2",
                          "filter": {
                            "type": "headerEquals",
                            "header": "region",
                            "value": "eu"
                          },
                          "exposePhysicalTopic": true
                        }
                        """);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2",
                        new HeaderEqualsFilterConfig("region", "eu"), true));
        assertThatJson(response.body()).isEqualTo("""
                {
                  "topic": "orders-v2",
                  "filter": {
                    "type": "headerEquals",
                    "header": "region",
                    "value": "eu"
                  },
                  "exposePhysicalTopic": true,
                  "valueFormat": null
                }
                """);
    }

    @Test
    void preservesOmittedVirtualTopicFieldsWhenPatching() throws Exception {
        // given
        var current = new VirtualTopicConfig("orders-v1", null, true);
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertVirtualTopic("orders", current));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders", "{\"topic\":\"orders-v2\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2", null, true));
    }

    @Test
    void clearsFilterWhenFilterIsNull() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v1",
                        new HeaderEqualsFilterConfig("region", "eu"), true)));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders", "{\"filter\":null}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v1", null, true));
    }

    @Test
    void clearsFilterWhenFilterIsOmitted() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v1",
                        new HeaderEqualsFilterConfig("region", "eu"), true)));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders", "{\"exposePhysicalTopic\":false}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v1", null, false));
    }

    @Test
    void renamesVirtualTopicWithoutCreatingAnotherEntry() throws Exception {
        // given
        var current = new VirtualTopicConfig("orders-v1", null, true);
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertVirtualTopic("orders", current));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders", "{\"name\":\"regional-orders\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsOnlyKeys("regional-orders")
                .containsEntry("regional-orders", current);
    }

    @Test
    void rejectsPatchForMissingVirtualTopic() throws Exception {
        // given
        startServer();

        // when
        var response = send("PATCH", "/topics/orders", "{\"topic\":\"orders-v2\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(repository.getActiveConfig().virtualTopics()).isEmpty();
        assertThat(repository.updateCalls()).isZero();
    }

    @Test
    void rejectsPatchingPhysicalTopic() throws Exception {
        // given
        cache = cacheWith(topic("orders", 1, 1));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders", "{\"topic\":\"orders-v2\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().virtualTopics()).isEmpty();
        assertThat(repository.updateCalls()).isZero();
    }

    @Test
    void rejectsPatchWhenRenamedVirtualTopicAlreadyExists() throws Exception {
        // given
        var original = new VirtualTopicConfig("orders-v1");
        var existing = new VirtualTopicConfig("customers-v1");
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertVirtualTopic("orders", original)
                .upsertVirtualTopic("customers", existing));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders", "{\"name\":\"customers\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", original)
                .containsEntry("customers", existing);
        assertThat(repository.updateCalls()).isZero();
    }

    @Test
    void rejectsInvalidPatchWithoutChangingExistingConfiguration() throws Exception {
        // given
        var current = new VirtualTopicConfig("orders-v1");
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertVirtualTopic("orders", current));
        startServer();

        // when
        var response = send("PATCH", "/topics/orders",
                "{\"topic\":\"\",\"filter\":{\"type\":\"unknown\"}}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", current);
        assertThat(repository.updateCalls()).isZero();
    }

    @Test
    void deletesPhysicalTopicOnBroker() throws Exception {
        // given
        cache = cacheWith(topic("orders", 1, 1));
        startServer();

        // when
        var response = send("DELETE", "/topics/orders", null);

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(topicAdmin.deleted).containsExactly("orders");
    }

    @Test
    void removesVirtualTopicConfigWithoutBrokerCall() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v2")));
        startServer();

        // when
        var response = send("DELETE", "/topics/orders", null);

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.getActiveConfig().virtualTopics()).isEmpty();
        assertThat(topicAdmin.deleted).isEmpty();
        assertThat(repository.updateCalls()).isEqualTo(1);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(0);
    }

    @Test
    void deletesVirtualTopicWithAppliedConsistencyWaitsForApplyMode() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v2")));
        startServer();

        // when
        var response = send("DELETE", "/topics/orders?consistency=applied", null);

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.updateCalls()).isEqualTo(0);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(1);
    }

    @Test
    void rejectsInvalidConsistencyForVirtualTopicDelete() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v2")));
        startServer();

        // when
        var response = send("DELETE", "/topics/orders?consistency=invalid", null);

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("invalid consistency 'invalid'");
        assertThat(repository.updateCalls()).isEqualTo(0);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(0);
    }

    @Test
    void unknownTopicReturnsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("DELETE", "/topics/orders", null);

        // then
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(topicAdmin.deleted).isEmpty();
    }

    @Test
    void virtualTopicWinsOverPhysicalName() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v2")));
        cache = cacheWith(topic("orders", 1, 1));
        startServer();

        // when
        var response = send("DELETE", "/topics/orders", null);

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.getActiveConfig().virtualTopics()).isEmpty();
        assertThat(topicAdmin.deleted).isEmpty();
    }
}
