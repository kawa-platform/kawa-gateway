package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.BootstrapConfig;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.config.ListenerConfig;
import io.jonasg.kawa.server.KafkaGateway;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/// Verifies that an empty config topic on first boot is valid: the gateway boots with the
/// static bootstrap's listeners/advertised/admin and empty dynamic consumers (no virtual
/// topics, default-deny RBAC, no client auth) instead of failing.
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EmptyConfigTopicIT {

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

        // Static bootstrap carries the startup-only config: listeners, advertised, admin.
        var bootstrap = new BootstrapConfig(
                List.of(new ListenerConfig("127.0.0.1", 0)),
                Map.of("default", new ClusterConfig(List.of(brokerBootstrap))),
                new AdvertisedListener(1, "localhost", 0),
                null,
                null,
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
    void bootsWithDefaultsOnEmptyConfigTopic() {
        // then - the gateway is up and serving on the static bootstrap's listener
        assertThat(gateway.boundPort()).isGreaterThan(0);
    }
}
