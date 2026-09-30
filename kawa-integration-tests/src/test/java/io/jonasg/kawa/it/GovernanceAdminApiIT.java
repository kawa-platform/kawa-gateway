package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.ListenerConfig;
import io.jonasg.kawa.server.KafkaGateway;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static io.jonasg.kawa.test.KawaAssertions.assertThat;

/// End-to-end flow for topic governance: a fresh gateway with an empty config topic gets
/// governance rules written over HTTP, and topic creation through `POST /topics` is then
/// enforced against the live policy. Exercises the full loop: admin HTTP write -> config
/// topic -> consumer apply -> live governance reload -> admission check.
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GovernanceAdminApiIT {

    static final String CONFIG_TOPIC = "__kawa";

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    private static KafkaGateway gateway;

    @BeforeAll
    void setUp() throws Exception {
        String brokerBootstrap = kafka.getBootstrapServers();

        // Create the config topic but write nothing to it - the empty-topic first boot case.
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap))) {
            admin.createTopics(List.of(new NewTopic(CONFIG_TOPIC, 1, (short) 1)
                    .configs(Map.of("cleanup.policy", "compact")))).all().get();
        }

        var bootstrap = new GatewayConfig(
                List.of(new ListenerConfig("127.0.0.1", 0)),
                Map.of("default", new ClusterConfig(List.of(brokerBootstrap))),
                null,
                new AdvertisedListener(1, "localhost", 0),
                new AuthConfig(null, null, null),
                null,
                new AdminConfig(true, "127.0.0.1", 0, null),
                CONFIG_TOPIC, null);

        gateway = new KafkaGateway(bootstrap);
        gateway.start();
    }

    @AfterAll
    void tearDown() {
        if (gateway != null) {
            gateway.stop();
        }
    }

    @Test
    @SuppressWarnings("resource")
    void enforcesGovernanceRulesOnTopicCreation() throws Exception {
        // given - a fresh gateway with an empty config topic and the admin HTTP server enabled
        String base = "http://127.0.0.1:" + gateway.adminBoundPort();
        var http = HttpClient.newHttpClient();

        // when - a governance rule with an exemption is written through the admin API. The admin
        // API's placeholder principal is `admin`, so the exemption covers its changelog topics.
        HttpResponse<String> governancePut = http.send(
                HttpRequest.newBuilder(URI.create(base + "/governance/rules/min-partitions"))
                        .PUT(HttpRequest.BodyPublishers.ofString("""
                                {
                                  "name": "min-partitions",
                                  "errorMessage": "partitions must be at least 2",
                                  "description": "All topics must have at least 2 partitions.",
                                  "selector": {
                                    "resourceType": "TOPIC"
                                  },
                                  "expression": {
                                    "type": "CEL",
                                    "value": "topic.partitions >= 2"
                                  },
                                  "exemptions": [
                                    {
                                      "name": "admin-changelogs",
                                      "description": "Changelog topics are sized by their stream.",
                                      "expression": {
                                        "type": "CEL",
                                        "value": "principal == 'admin' && topic.name.endsWith('-changelog')"
                                      }
                                    }
                                  ]
                                }
                                """))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then - the write is persisted and readable
        assertThat(governancePut).hasStatusCode(200);
        HttpResponse<String> governanceGet = http.send(
                HttpRequest.newBuilder(URI.create(base + "/governance/rules")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(governanceGet).hasStatusCode(200);
        assertThat(governanceGet).hasBody("""
                {
                  "rules": [
                    {
                      "name": "min-partitions",
                      "errorMessage": "partitions must be at least 2",
                      "description": "All topics must have at least 2 partitions.",
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
                          "name": "admin-changelogs",
                          "description": "Changelog topics are sized by their stream.",
                          "expression": {
                            "type": "CEL",
                            "value": "principal == 'admin' && topic.name.endsWith('-changelog')"
                          }
                        }
                      ]
                    }
                  ]
                }
                """);

        // when - a topic violating the rule is triggered
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    HttpResponse<String> rejected = http.send(
                            HttpRequest.newBuilder(URI.create(base + "/topics"))
                                    .POST(HttpRequest.BodyPublishers.ofString(
                                            "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":1,\"replicationFactor\":1}"))
                                    .build(),
                            HttpResponse.BodyHandlers.ofString());
                    assertThat(rejected).hasStatusCode(403);
                    assertThat(rejected.body()).contains("partitions must be at least 2");
                });

        // then - an exempt topic is admitted despite violating the rule
        HttpResponse<String> exempt = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics"))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"type\":\"physical\",\"name\":\"orders-changelog\",\"partitions\":1,\"replicationFactor\":1}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(exempt).hasStatusCode(201);

        // and - a compliant topic is admitted
        HttpResponse<String> admitted = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics"))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":1}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(admitted).hasStatusCode(201);
    }
}
