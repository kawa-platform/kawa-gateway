package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.BootstrapConfig;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.config.ListenerConfig;
import io.jonasg.kawa.server.KafkaGateway;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.SaslConfigs;
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
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end flow for the admin config API: a fresh gateway with an empty config topic
/// (no users, no RBAC) gets its first user, role and group added over HTTP, and a client
/// authenticating as that user can then talk to the cluster. Exercises the full loop:
/// admin HTTP write -> config topic -> consumer apply -> live SASL/RBAC reload.
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AdminConfigApiIT {

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

        var bootstrap = new BootstrapConfig(
                List.of(new ListenerConfig("127.0.0.1", 0)),
                Map.of("default", new ClusterConfig(List.of(brokerBootstrap))),
                new AdvertisedListener(1, "localhost", 0),
                null,
                new AdminConfig(true, "127.0.0.1", 0, null),
                CONFIG_TOPIC);

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
    void addsFirstUserRoleAndGroupViaAdminApi() throws Exception {
        // given - a fresh gateway with an empty config topic and the admin HTTP server enabled
        String base = "http://127.0.0.1:" + gateway.adminBoundPort();
        var http = HttpClient.newHttpClient();

        // when - the first user, role and group are added through the admin API
        HttpResponse<String> userPut = http.send(
                HttpRequest.newBuilder(URI.create(base + "/auth/clients/alice"))
                        .PUT(HttpRequest.BodyPublishers.ofString("{\"mechanism\":\"PLAIN\",\"password\":\"secret\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> rolePut = http.send(
                HttpRequest.newBuilder(URI.create(base + "/rbac/roles/allow-all"))
                        .PUT(HttpRequest.BodyPublishers.ofString("""
                                {"acls":[
                                  {"resource":{"type":"TOPIC","pattern":"","patternType":"PREFIXED"},"operation":"ALL"},
                                  {"resource":{"type":"GROUP","pattern":"","patternType":"PREFIXED"},"operation":"ALL"},
                                  {"resource":{"type":"TRANSACTIONAL_ID","pattern":"","patternType":"PREFIXED"},"operation":"ALL"},
                                  {"resource":{"type":"CLUSTER"},"operation":"ALL"}
                                ]}
                                """))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> groupPut = http.send(
                HttpRequest.newBuilder(URI.create(base + "/rbac/groups/admins"))
                        .PUT(HttpRequest.BodyPublishers.ofString("{\"clients\":[\"alice\"],\"roles\":[\"allow-all\"]}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then - every write is persisted
        assertThat(userPut.statusCode()).isEqualTo(200);
        assertThat(rolePut.statusCode()).isEqualTo(200);
        assertThat(groupPut.statusCode()).isEqualTo(200);

        // when - a client authenticates as the new user and describes the cluster
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:" + gateway.boundPort());
        props.put(SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"alice\" password=\"secret\";");
        try (var admin = AdminClient.create(props)) {
            // then - the consumer applies the snapshot and the client can talk to the cluster
            Awaitility.await()
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofMillis(500))
                    .untilAsserted(() ->
                            assertThat(admin.describeCluster().nodes().get()).isNotEmpty());
        }
    }
}
