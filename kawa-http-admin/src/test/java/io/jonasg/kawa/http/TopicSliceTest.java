package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminAuthConfig;
import io.jonasg.kawa.config.AvroFormatConfig;
import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.DecodeErrorPolicy;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig.Expression;
import io.jonasg.kawa.config.GovernanceRuleConfig.Selector;
import io.jonasg.kawa.config.HeaderContainsFilterConfig;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.HeaderMatchesFilterConfig;
import io.jonasg.kawa.config.HeaderStartsWithFilterConfig;
import io.jonasg.kawa.config.JsonFormatConfig;
import io.jonasg.kawa.config.ManagedBy;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.governance.TopicSpec;
import io.jonasg.kawa.http.auth.builtin.BuiltInAuthenticationPlugin;
import io.jonasg.kawa.plugin.KawaPlugin;
import io.jonasg.kawa.plugin.KawaPluginContext;
import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static net.javacrumbs.jsonunit.core.Option.IGNORING_ARRAY_ORDER;

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
                            "exposePhysicalTopic": false,
                            "filter": null,
                            "physicalTopic": "orders-v2",
                            "valueFormat": null,
                            "managedBy": null
                          },
                          {
                            "type": "physical",
                            "name": "orders-v2",
                            "partitions": 3,
                            "replicationFactor": 2,
                            "filter": null,
                            "physicalTopic": null,
                            "exposePhysicalTopic": null,
                            "valueFormat": null,
                            "managedBy": null
                          },
                          {
                            "type": "virtual",
                            "name": "customers",
                            "partitions": 2,
                            "replicationFactor": 3,
                            "exposePhysicalTopic": true,
                            "filter": {
                              "kind": "header",
                              "expression": "tenant=acme"
                            },
                            "physicalTopic": "crm.customers",
                            "valueFormat": null,
                            "managedBy": null
                          },
                          {
                            "type": "physical",
                            "name": "crm.customers",
                            "partitions": 2,
                            "replicationFactor": 3,
                            "filter": null,
                            "physicalTopic": null,
                            "exposePhysicalTopic": null,
                            "valueFormat": null,
                            "managedBy": null
                          },
                          {
                            "type": "physical",
                            "name": "raw-events",
                            "partitions": 1,
                            "replicationFactor": 1,
                            "filter": null,
                            "physicalTopic": null,
                            "exposePhysicalTopic": null,
                            "valueFormat": null,
                            "managedBy": null
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
                  "exposePhysicalTopic": false,
                  "filter": {
                    "kind": "cel",
                    "expression": "headers.tenant == \\\"acme\\\""
                  },
                  "physicalTopic": "audit-v1",
                  "valueFormat": null,
                  "managedBy": null
                },
                {
                  "type": "physical",
                  "name": "audit-v1",
                  "partitions": 1,
                  "replicationFactor": 1,
                  "filter": null,
                  "physicalTopic": null,
                  "exposePhysicalTopic": null,
                  "valueFormat": null,
                  "managedBy": null
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
                            "exposePhysicalTopic": false,
                            "filter": {
                              "kind": "headerContains",
                              "expression": "tenant contains acm"
                            },
                            "physicalTopic": "contains-v1",
                            "valueFormat": null,
                            "managedBy": null
                          },
                          {
                            "type": "virtual",
                            "name": "startsWith",
                            "partitions": 1,
                            "replicationFactor": 1,
                            "exposePhysicalTopic": false,
                            "filter": {
                              "kind": "headerStartsWith",
                              "expression": "tenant starts with ac"
                            },
                            "physicalTopic": "starts-v1",
                            "valueFormat": null,
                            "managedBy": null
                          },
                          {
                            "type": "virtual",
                            "name": "matches",
                            "partitions": 1,
                            "replicationFactor": 1,
                            "exposePhysicalTopic": false,
                            "filter": {
                              "kind": "headerMatches",
                              "expression": "tenant matches eu.*"
                            },
                            "physicalTopic": "matches-v1",
                            "valueFormat": null,
                            "managedBy": null
                          },
                          {
                          "type": "physical",
                          "name": "contains-v1",
                          "partitions": 1,
                          "replicationFactor": 1,
                          "filter": null,
                          "physicalTopic": null,
                          "exposePhysicalTopic": null,
                          "valueFormat": null,
                          "managedBy": null
                          },
                          {
                          "type": "physical",
                          "name": "starts-v1",
                          "partitions": 1,
                          "replicationFactor": 1,
                          "filter": null,
                          "physicalTopic": null,
                          "exposePhysicalTopic": null,
                          "valueFormat": null,
                          "managedBy": null
                          },
                          {
                          "type": "physical",
                          "name": "matches-v1",
                          "partitions": 1,
                          "replicationFactor": 1,
                          "filter": null,
                          "physicalTopic": null,
                          "exposePhysicalTopic": null,
                          "valueFormat": null,
                          "managedBy": null
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
                  "exposePhysicalTopic": false,
                  "filter": null,
                  "physicalTopic": "orders-v2",
                  "valueFormat": null,
                  "managedBy": null
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
    void rejectsVirtualTopicWithoutName() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/topics", "{\"type\":\"virtual\",\"topic\":\"orders-v2\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("topic name is required");
    }

    @Test
    void rejectsTopicViolatingGovernance() throws Exception {
        // given
        governance = new GovernancePolicy(new GovernanceConfig(Map.of("min-replication", minReplicationRule())));
        startServer();

        // when
        var response = send("POST", "/topics",
                "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":1}");

        // then
        assertThat(response).hasStatusCode(403);
        assertThat(response.body()).contains("[min-replication] replication factor must be at least 3");
        assertThat(topicAdmin.created).isEmpty();
    }

    @Test
    void acceptsExemptTopicAndCreatesOnBroker() throws Exception {
        // given - the admin API's placeholder principal is `admin`
        governance = new GovernancePolicy(new GovernanceConfig(Map.of("min-replication", minReplicationRule(
                new GovernanceRuleConfig.Exemption(
                        "ops",
                        "Operators may create under-replicated topics.",
                        Expression.cel("principal == 'admin'"))))));
        startServer();

        // when
        var response = send("POST", "/topics",
                "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":1}");

        // then
        assertThat(response).hasStatusCode(201);
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
    void createsVirtualTopicWithAvroValueFormat() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/topics", """
                {
                  "type": "virtual",
                  "name": "eu-orders",
                  "topic": "orders",
                  "filter": {"type": "cel", "expression": "value.region == \\"eu\\""},
                  "valueFormat": {"type": "avro"}
                }
                """);

        // then
        assertThat(response.statusCode()).isEqualTo(201);
        assertThat(repository.getActiveConfig().virtualTopics())
                .withFailMessage(() -> "Virtual topic was not persisted with its Avro value format")
                .containsEntry("eu-orders", new VirtualTopicConfig(
                        "orders",
                        new CelFilterConfig("value.region == \"eu\""),
                        false,
                        new AvroFormatConfig(DecodeErrorPolicy.SKIP)));
        assertThatJson(response.body()).inPath("valueFormat").isEqualTo("""
                {"type": "avro", "onDecodeError": "skip"}
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
    }

    @Test
    void updatesVirtualTopicWithAppliedConsistencyWaitsForApplyMode() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/topics/orders?consistency=applied", "{\"type\":\"virtual\",\"topic\":\"raw-orders\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
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
                          "exposePhysicalTopic": false
                        }
                        """);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2",
                        new HeaderEqualsFilterConfig("region", "eu"), false));
        assertThatJson(response.body()).isEqualTo("""
                {
                  "topic": "orders-v2",
                  "filter": {
                    "type": "headerEquals",
                    "header": "region",
                    "value": "eu"
                  },
                  "exposePhysicalTopic": false,
                  "valueFormat": null,
                  "managedBy": null
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

    @Test
    void listsTopicsWithExactlyTheNineTopicPropertyNames() throws Exception {
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
        var topicsResp = send("GET", "/topics");

        // then
        assertThat(topicsResp.statusCode()).isEqualTo(200);
        assertThat(topicsResp).containsExactlyTopLevelPropertyNames(
                "type", "name", "partitions", "replicationFactor", "filter", "physicalTopic",
                "exposePhysicalTopic", "valueFormat", "managedBy");
    }

    @Test
    void postVirtualResponseOmitsNameAndType() throws Exception {
        // given
        startServer();

        // when
        var postResp = send("POST", "/topics", "{\"type\":\"virtual\",\"name\":\"orders\",\"topic\":\"orders-v2\"}");

        // then
        assertThat(postResp.statusCode()).isEqualTo(201);
        assertThat(postResp).containsExactlyTopLevelPropertyNames("topic", "filter", "exposePhysicalTopic", "valueFormat", "managedBy");
    }

    @Test
    void postPhysicalResponseOmitsType() throws Exception {
        // given
        startServer();

        // when
        var postResp = send("POST", "/topics",
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
        assertThat(postResp.statusCode()).isEqualTo(201);
        assertThat(postResp).containsExactlyTopLevelPropertyNames("name", "partitions", "replicationFactor", "configs");
    }

    @Test
    void putResponseOmitsNameAndType() throws Exception {
        // given
        startServer();

        // when
        var putResp = send("PUT", "/topics/orders", "{\"type\":\"virtual\",\"topic\":\"raw-orders\"}");

        // then
        assertThat(putResp.statusCode()).isEqualTo(200);
        assertThat(putResp).containsExactlyTopLevelPropertyNames("topic", "filter", "exposePhysicalTopic", "valueFormat", "managedBy");
    }

    @Test
    void patchResponseOmitsNameAndType() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v1")));
        startServer();

        // when
        var patchResp = send("PATCH", "/topics/orders", "{\"topic\":\"orders-v2\"}");

        // then
        assertThat(patchResp.statusCode()).isEqualTo(200);
        assertThat(patchResp).containsExactlyTopLevelPropertyNames("topic", "filter", "exposePhysicalTopic", "valueFormat", "managedBy");
    }

    private static GovernanceRuleConfig minReplicationRule(GovernanceRuleConfig.Exemption... exemptions) {
        return new GovernanceRuleConfig(
                "min-replication",
                "replication factor must be at least 3",
                "Topics need at least 3 replicas to survive a broker loss.",
                Selector.topic(),
                Expression.cel("topic.replicationFactor >= 3"),
                List.of(exemptions));
    }

    @Test
    void judgesAVirtualTopicAsThePrincipalADelegateActsFor() throws Exception {
        // given: kawa-operator may act as k8s:*, and only k8s:team-a may create virtual topics
        auth = operatorAuth();
        governance = new GovernancePolicy(new GovernanceConfig(Map.of("teams", new GovernanceRuleConfig(
                "teams", "only team-a manages virtual topics here", null,
                new Selector(ResourceType.TOPIC, null, GovernanceRuleConfig.TopicScope.VIRTUAL,
                        Set.of(GovernanceRuleConfig.Operation.CREATE)),
                Expression.cel("principal == 'k8s:team-a'")))));
        startServer();
        String teamA = delegatedToken("k8s:team-a");
        String teamB = delegatedToken("k8s:team-b");
        String body = """
                {"type": "virtual", "topic": "orders-v2"}
                """;

        // when
        var teamBResp = send("PUT", "/topics/orders", body, bearer(teamB));
        var teamAResp = send("PUT", "/topics/orders", body, bearer(teamA));

        // then
        assertThat(teamBResp).hasStatusCode(403);
        assertThat(teamBResp.body()).contains("only team-a manages virtual topics here");
        assertThat(teamAResp).hasStatusCode(200);
    }

    @Test
    void anOperatorTokenReadsWritesAndDeletesVirtualTopics() throws Exception {
        // given: the operator authenticates with the client credentials grant
        auth = operatorAuth();
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertVirtualTopic("old-orders", new VirtualTopicConfig("orders-v1")));
        startServer();
        var operator = bearer(operatorToken());

        // when
        var listed = send("GET", "/topics", null, operator);
        var put = send("PUT", "/topics/orders?consistency=applied", "{\"type\":\"virtual\",\"topic\":\"orders-v2\"}", operator);
        var deleted = send("DELETE", "/topics/old-orders?consistency=applied", null, operator);

        // then
        assertThat(listed).hasStatusCode(200);
        assertThat(put).hasStatusCode(200);
        assertThat(deleted).hasStatusCode(204);
        assertThat(repository.getActiveConfig().virtualTopics())
                .withFailMessage(() -> "the operator's PUT and DELETE were not applied to the gateway config")
                .containsOnlyKeys("orders");
    }

    @Test
    void judgesAVirtualTopicAsTheOperatorWhenItActsForItself() throws Exception {
        // given: only the principal 'kawa-operator' may create virtual topics
        auth = operatorAuth();
        governance = new GovernancePolicy(new GovernanceConfig(Map.of("operator", new GovernanceRuleConfig(
                "operator", "only the operator manages virtual topics here", null,
                new Selector(ResourceType.TOPIC, null, GovernanceRuleConfig.TopicScope.VIRTUAL,
                        Set.of(GovernanceRuleConfig.Operation.CREATE)),
                Expression.cel("principal == 'kawa-operator'")))));
        startServer();

        // when
        var response = send("PUT", "/topics/orders", "{\"type\":\"virtual\",\"topic\":\"orders-v2\"}", bearer(operatorToken()));

        // then
        assertThat(response)
                .withFailMessage(() -> "governance did not see the principal 'kawa-operator': " + response.body())
                .hasStatusCode(200);
    }

    @Test
    void anOperatorWriteMarksTheVirtualTopicAsManagedByTheOperator() throws Exception {
        // given
        auth = operatorAuth();
        startServer();

        // when
        var putResp = send("PUT", "/topics/orders", "{\"type\":\"virtual\",\"topic\":\"orders-v2\"}", bearer(operatorToken()));

        // then
        assertThat(putResp).hasStatusCode(200);
        assertThatJson(putResp.body()).inPath("managedBy").isEqualTo("{\"principal\":\"kawa-operator\"}");
        assertThat(repository.getActiveConfig().virtualTopics().get("orders").managedBy())
                .withFailMessage(() -> "virtual topic 'orders' was not stamped as managed by the operator")
                .isEqualTo(new ManagedBy("kawa-operator"));
    }

    @Test
    void anOperatorWriteForADelegatedPrincipalIsManagedByTheOperator() throws Exception {
        // given
        auth = operatorAuth();
        startServer();

        // when
        var putResp = send("PUT", "/topics/orders", "{\"type\":\"virtual\",\"topic\":\"orders-v2\"}",
                bearer(delegatedToken("k8s:team-a")));

        // then
        assertThat(putResp).hasStatusCode(200);
        assertThat(repository.getActiveConfig().virtualTopics().get("orders").managedBy())
                .withFailMessage(() -> "a write the operator made for k8s:team-a was not managed by the operator")
                .isEqualTo(new ManagedBy("kawa-operator"));
    }

    @Test
    void listsTheOwnerOfAManagedVirtualTopic() throws Exception {
        // given
        virtualTopics = new VirtualTopicManager(Map.of("orders", new VirtualTopicConfig("orders-v2")));
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty().upsertVirtualTopic("orders", managedOrders()));
        startServer();

        // when
        var topicsResp = send("GET", "/topics", null);

        // then
        assertThat(topicsResp).hasStatusCode(200);
        assertThatJson(topicsResp.body()).inPath("[0].managedBy").isEqualTo("{\"principal\":\"kawa-operator\"}");
    }

    @Test
    void refusesChangesToAManagedVirtualTopicByAnyoneButItsOwner() throws Exception {
        // given
        auth = operatorAuth();
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty().upsertVirtualTopic("orders", managedOrders()));
        startServer();
        var studio = bearer(login("admin@kawa", "s3cret"));

        // when
        var putResp = send("PUT", "/topics/orders", "{\"type\":\"virtual\",\"topic\":\"orders-v3\"}", studio);
        var postResp = send("POST", "/topics", "{\"type\":\"virtual\",\"name\":\"orders\",\"topic\":\"orders-v3\"}", studio);
        var patchResp = send("PATCH", "/topics/orders", "{\"name\":\"orders-renamed\"}", studio);
        var deleteResp = send("DELETE", "/topics/orders", null, studio);

        // then
        for (var response : List.of(putResp, postResp, patchResp, deleteResp)) {
            assertThat(response).hasStatusCode(409);
            assertThatJson(response.body()).isEqualTo("""
                    {"error": "topic 'orders' is managed by kawa-operator; change it through Kubernetes"}
                    """);
        }
        assertThat(repository.getActiveConfig().virtualTopics())
                .withFailMessage(() -> "a refused write changed managed virtual topic 'orders': " + repository.getActiveConfig().virtualTopics())
                .containsExactly(Map.entry("orders", managedOrders()));
    }

    @Test
    void theOwnerRewritesItsManagedVirtualTopic() throws Exception {
        // given
        auth = operatorAuth();
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty().upsertVirtualTopic("orders", managedOrders()));
        startServer();

        // when
        var putResp = send("PUT", "/topics/orders", "{\"type\":\"virtual\",\"topic\":\"orders-v3\"}", bearer(operatorToken()));

        // then
        assertThat(putResp).hasStatusCode(200);
        assertThat(repository.getActiveConfig().virtualTopics().get("orders"))
                .withFailMessage(() -> "the operator could not rewrite its virtual topic 'orders'")
                .isEqualTo(new VirtualTopicConfig("orders-v3", null, false, null, new ManagedBy("kawa-operator")));
    }

    @Test
    void aStudioWriteLeavesAnUnmanagedVirtualTopicUnmanaged() throws Exception {
        // given
        auth = operatorAuth();
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty().upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v2")));
        startServer();

        // when
        var putResp = send("PUT", "/topics/orders", "{\"type\":\"virtual\",\"topic\":\"orders-v3\"}", bearer(login("admin@kawa", "s3cret")));

        // then
        assertThat(putResp).hasStatusCode(200);
        assertThatJson(putResp.body()).inPath("managedBy").isNull();
        assertThat(repository.getActiveConfig().virtualTopics().get("orders"))
                .withFailMessage(() -> "a studio write did not leave virtual topic 'orders' unmanaged")
                .isEqualTo(new VirtualTopicConfig("orders-v3"));
    }

    @Test
    void onlyTheOwnerReleasesAManagedVirtualTopic() throws Exception {
        // given
        auth = operatorAuth();
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty().upsertVirtualTopic("orders", managedOrders()));
        startServer();

        // when
        var studioReleaseResp = send("DELETE", "/topics/orders/managed-by", null, bearer(login("admin@kawa", "s3cret")));
        var operatorReleaseResp = send("DELETE", "/topics/orders/managed-by", null, bearer(operatorToken()));

        // then
        assertThat(studioReleaseResp).hasStatusCode(409);
        assertThatJson(studioReleaseResp.body()).isEqualTo("""
                {"error": "topic 'orders' is managed by kawa-operator; change it through Kubernetes"}
                """);
        assertThat(operatorReleaseResp).hasStatusCode(204);
        assertThat(repository.getActiveConfig().virtualTopics().get("orders"))
                .withFailMessage(() -> "the operator's release did not clear the owner of virtual topic 'orders'")
                .isEqualTo(new VirtualTopicConfig("orders-v2"));
    }

    @Test
    void releasingAnUnmanagedVirtualTopicIsANoOpAndAMissingOneIsNotFound() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty().upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v2")));
        startServer();

        // when
        var unmanagedResp = send("DELETE", "/topics/orders/managed-by", null);
        var missingResp = send("DELETE", "/topics/missing/managed-by", null);

        // then
        assertThat(unmanagedResp).hasStatusCode(204);
        assertThat(missingResp).hasStatusCode(404);
    }

    @Test
    void aCallerMerelyNamedKawaOperatorDoesNotOwnManagedVirtualTopics() throws Exception {
        // given: a plugin provider authenticates a caller named 'kawa-operator' that is not the
        // operator client; admin usernames are email addresses, but a provider may name anyone
        authenticationProviders = registryOf(new BuiltInAuthenticationPlugin(), new NamedPrincipalPlugin("kawa-operator"));
        auth = new AdminAuthConfig(NamedPrincipalPlugin.TYPE, null, Map.of());
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty().upsertVirtualTopic("orders", managedOrders()));
        startServer();

        // when
        var putResp = send("PUT", "/topics/orders", "{\"type\":\"virtual\",\"topic\":\"orders-v3\"}");
        var releaseResp = send("DELETE", "/topics/orders/managed-by", null);

        // then
        for (var response : List.of(putResp, releaseResp)) {
            assertThat(response).hasStatusCode(409);
            assertThatJson(response.body()).isEqualTo("""
                    {"error": "topic 'orders' is managed by kawa-operator; change it through Kubernetes"}
                    """);
        }
        assertThat(repository.getActiveConfig().virtualTopics())
                .withFailMessage(() -> "a caller named 'kawa-operator' changed managed virtual topic 'orders'")
                .containsExactly(Map.entry("orders", managedOrders()));
    }

    private static VirtualTopicConfig managedOrders() {
        return new VirtualTopicConfig("orders-v2", null, false, null, new ManagedBy("kawa-operator"));
    }

    /// A plugin provider that authenticates every request as a principal called `name`, acting for
    /// itself (no actor).
    private record NamedPrincipalPlugin(String name) implements KawaPlugin {

        static final String TYPE = "test-named-principal";

        @Override
        public void register(KawaPluginContext context) {
            context.registerAuthenticationProvider(new AuthenticationProviderFactory() {
                @Override
                public String name() {
                    return TYPE;
                }

                @Override
                public AuthenticationProvider create(AuthenticationProviderContext providerContext) {
                    return request -> Optional.of(new KawaPrincipal(NamedPrincipalPlugin.this.name()));
                }
            });
        }
    }
}
