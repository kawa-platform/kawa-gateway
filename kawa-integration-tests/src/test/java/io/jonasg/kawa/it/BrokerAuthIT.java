package io.jonasg.kawa.it;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.ListenerConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.server.KafkaGateway;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import tools.jackson.databind.json.JsonMapper;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/// Wire-level check that the gateway authenticates to a SASL-enabled upstream broker
/// using the configured broker credentials, instead of connecting in plaintext.
///
/// Uses [GenericContainer] with `confluentinc/cp-kafka` rather than
/// `KafkaContainer` so we have full control over listener configuration and SASL setup.
/// The container's command is overridden to inject a JAAS config file before starting Kafka.
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BrokerAuthIT {

    static final String BROKER_USER = "gateway";
    static final String BROKER_PASSWORD = "gateway-secret";
    static final int SASL_PORT = 19093;

    /// The broker's SASL listener is bound to a dynamic host port (instead of a
    /// fixed one) so the test does not collide with other processes on shared CI
    /// runners. The advertised listener (`localhost:<hostPort>`) is what the
    /// gateway connects to.
    static final int HOST_SASL_PORT = freePort();
    static GenericContainer<?> kafka = new GenericContainer<>("confluentinc/cp-kafka:7.6.0")
            .withEnv("KAFKA_NODE_ID", "1")
            .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
            .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
            .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                    "CONTROLLER:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT")
            .withEnv("KAFKA_LISTENERS",
                    "CONTROLLER://0.0.0.0:29093,SASL_PLAINTEXT://0.0.0.0:" + SASL_PORT)
            .withEnv("KAFKA_ADVERTISED_LISTENERS",
                    "SASL_PLAINTEXT://localhost:" + HOST_SASL_PORT)
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
            .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "SASL_PLAINTEXT")
            .withEnv("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
            .withCreateContainerCmdModifier(cmd -> {
                // Docker publishes a host port only for a container port that is *exposed*; a
                // PortBindings entry for an unexposed port is dropped silently. The cp-kafka image
                // exposes 9092/9093, not this SASL listener's port, so it has to be added here or
                // the binding below never takes effect and the host can't reach the broker.
                cmd.withExposedPorts(ExposedPort.tcp(SASL_PORT));
                cmd.getHostConfig().withPortBindings(new PortBinding(
                        Ports.Binding.bindPort(HOST_SASL_PORT),
                        ExposedPort.tcp(SASL_PORT)));
            })
            .withCommand("bash", "-c",
                    "mkdir -p /etc/kafka/secrets && "
                    + "cat > /etc/kafka/secrets/kafka_server_jaas.conf << 'EOF'\n"
                    + "KafkaServer {\n"
                    + "  org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                    + "  username=\"" + BROKER_USER + "\"\n"
                    + "  password=\"" + BROKER_PASSWORD + "\"\n"
                    + "  user_" + BROKER_USER + "=\"" + BROKER_PASSWORD + "\";\n"
                    + "};\n"
                    + "EOF\n"
                    + "export KAFKA_OPTS='-Djava.security.auth.login.config=/etc/kafka/secrets/kafka_server_jaas.conf' && "
                    + "/etc/confluent/docker/run")
            .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1))
            .withStartupTimeout(Duration.ofMinutes(2));

    private static KafkaGateway gateway;
    private static String gatewayBootstrap;

    @BeforeAll
    void setUp() throws Exception {
        kafka.start();
        awaitSaslListener(kafka);
        String brokerBootstrap = "localhost:" + HOST_SASL_PORT;

        var auth = new AuthConfig(
                Set.of("PLAIN"),
                Map.of("client", new ClientConfig("PLAIN",
                        io.jonasg.kawa.config.HashedPassword.fromPlaintext(
                                io.jonasg.kawa.config.Mechanism.PLAIN, "client-secret"))),
                new BrokerAuthConfig("PLAIN", BROKER_USER, BROKER_PASSWORD));

        var allowAllRole = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.GROUP, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.CLUSTER, null), AclOperation.ALL)));
        var rbac = new RbacConfig(
                Map.of("allow-all", allowAllRole),
                Map.of("clients", new GroupConfig(List.of("client"), List.of("allow-all"))));

        // Create the config topic and write the dynamic snapshot (client auth + RBAC) before
        // the gateway starts; the gateway gets the static bootstrap (listeners, advertised,
        // admin, broker auth) from the file.
        try (var admin = AdminClient.create(saslBrokerProps(brokerBootstrap))) {
            admin.createTopics(List.of(new NewTopic(CONFIG_TOPIC, 1, (short) 1)
                    .configs(Map.of("cleanup.policy", "compact")))).all().get();
        }
        writeConfigSnapshot(brokerBootstrap, auth, rbac);

        var bootstrap = new GatewayConfig(
                List.of(new ListenerConfig("127.0.0.1", 0)),
                Map.of("default", new ClusterConfig(List.of(brokerBootstrap))),
                null,
                new AdvertisedListener(1, "localhost", 0),
                new AuthConfig(null, null, new BrokerAuthConfig("PLAIN", BROKER_USER, BROKER_PASSWORD)),
                null,
                null,
                CONFIG_TOPIC, null);

        gateway = new KafkaGateway(bootstrap);
        try {
            gateway.start();
        } catch (Exception e) {
            throw new IllegalStateException("Gateway failed to start; broker logs:\n" + kafka.getLogs(), e);
        }
        gatewayBootstrap = "localhost:" + gateway.boundPort();
    }

    @AfterAll
    void tearDown() {
        if (gateway != null) {
            gateway.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
    }

    /// Waits until the broker's SASL listener accepts TCP connections on the host
    /// port. The "Kafka Server started" log line can appear slightly before the
    /// listener is actually reachable, and on slow CI runners that gap can be
    /// significant.
    private static void awaitSaslListener(GenericContainer<?> container) {
        try {
            Awaitility.await("SASL listener on localhost:" + HOST_SASL_PORT)
                    .atMost(Duration.ofSeconds(60))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> canConnect(HOST_SASL_PORT));
        } catch (ConditionTimeoutException e) {
            throw new IllegalStateException(
                    "Broker SASL listener never became reachable on localhost:" + HOST_SASL_PORT
                    + "; container logs:\n" + container.getLogs(), e);
        }
    }

    private static boolean canConnect(int port) {
        try {
            for (var address : InetAddress.getAllByName("localhost")) {
                try (var socket = new Socket(address, port)) {
                    return true;
                } catch (IOException ignored) {
                    // try the next resolved address
                }
            }
            return false;
        } catch (IOException e) {
            return false;
        }
    }

    /// Allocates a free ephemeral port and releases it again; the container binds it
    /// shortly after, so the race window is negligible.
    private static int freePort() {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Could not allocate a free port for the SASL listener", e);
        }
    }

    static final String CONFIG_TOPIC = "__kawa";

    /// `Properties` that authenticate to the SASL broker as the gateway's own broker user.
    private static Properties saslBrokerProps(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put(SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"" + BROKER_USER + "\" password=\"" + BROKER_PASSWORD + "\";");
        return props;
    }

    /// Serializes the dynamic config (client auth + RBAC) as JSON and writes it to the config
    /// topic, so the gateway's initial catch-up finds it at boot. Listeners, advertised and
    /// admin are startup-only and come from the static bootstrap.
    private static void writeConfigSnapshot(String brokerBootstrap, AuthConfig auth, RbacConfig rbac) throws Exception {
        var fullConfig = new GatewayConfig(
                null,
                Map.of("default", new ClusterConfig(List.of(brokerBootstrap))),
                Map.of(),
                null,
                auth,
                rbac,
                null,
                CONFIG_TOPIC, null);
        var mapper = JsonMapper.builder().build();
        String json = mapper.writeValueAsString(fullConfig);
        Properties props = saslBrokerProps(brokerBootstrap);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (var producer = new KafkaProducer<String, String>(props)) {
            producer.send(new ProducerRecord<>(CONFIG_TOPIC, "test-gateway", json)).get();
        }
    }

    @Test
    void gatewayProducesThroughSaslAuthenticatedBrokerConnection() throws Exception {
        var producerProps = new Properties();
        producerProps.put("bootstrap.servers", gatewayBootstrap);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        producerProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        producerProps.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"client\" password=\"client-secret\";");

        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerProps)) {
            producer.send(new ProducerRecord<>("broker-auth-test", "key", "value")).get();
        }

        var consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", gatewayBootstrap);
        consumerProps.put("group.id", "broker-auth-it");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        consumerProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        consumerProps.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"client\" password=\"client-secret\";");

        try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(consumerProps)) {
            consumer.subscribe(List.of("broker-auth-test"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records).hasSize(1);
            assertThat(records.iterator().next().value()).isEqualTo("value");
        }
    }
}
