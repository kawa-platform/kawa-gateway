package io.jonasg.kawa.it;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.ListenerConfig;
import io.jonasg.kawa.config.Mechanism;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.config.StoreConfig;
import io.jonasg.kawa.config.StoreType;
import io.jonasg.kawa.config.TlsConfig;
import io.jonasg.kawa.server.KafkaGateway;
import io.jonasg.kawa.test.TestTls;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The gateway reaching a broker over TLS: SSL with mutual TLS, and SASL_SSL. Every connection the
/// gateway opens to the cluster (the config-topic consumer and writer, the metadata and broker
/// connections, and the topic admin) must speak TLS, or a TLS-only cluster is unreachable.
///
/// The broker (`cp-kafka`, as in [BrokerAuthIT]) uses the test certificates from
/// [TestTls]: its certificate names `localhost`, which is where the gateway reaches it.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BrokerTlsIT {

    private static final String CONFIG_TOPIC = "__kawa";
    private static final String BROKER_USER = "gateway";
    private static final String BROKER_PASSWORD = "gateway-secret";
    private static final String CLIENT = "client";
    private static final String CLIENT_PASSWORD = "client-secret";
    private static final int SSL_PORT = 19094;
    private static final int SASL_SSL_PORT = 19095;
    private static final int HOST_SSL_PORT = freePort();
    private static final int HOST_SASL_SSL_PORT = freePort();
    /// The container runs as a non-root user; temp files are owner-only by default.
    private static final int READABLE = 0644;

    private final GenericContainer<?> kafka = new GenericContainer<>("confluentinc/cp-kafka:7.6.0")
            .withEnv("KAFKA_NODE_ID", "1")
            .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
            .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
            .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                    "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,SSL:SSL,SASL_SSL:SASL_SSL")
            .withEnv("KAFKA_LISTENERS", "CONTROLLER://0.0.0.0:29093,INTERNAL://0.0.0.0:29092,"
                    + "SSL://0.0.0.0:" + SSL_PORT + ",SASL_SSL://0.0.0.0:" + SASL_SSL_PORT)
            .withEnv("KAFKA_ADVERTISED_LISTENERS", "INTERNAL://localhost:29092,"
                    + "SSL://localhost:" + HOST_SSL_PORT + ",SASL_SSL://localhost:" + HOST_SASL_SSL_PORT)
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
            .withEnv("KAFKA_SSL_KEYSTORE_FILENAME", "broker.p12")
            .withEnv("KAFKA_SSL_KEYSTORE_TYPE", "PKCS12")
            .withEnv("KAFKA_SSL_KEYSTORE_CREDENTIALS", "credentials")
            .withEnv("KAFKA_SSL_KEY_CREDENTIALS", "credentials")
            .withEnv("KAFKA_SSL_TRUSTSTORE_FILENAME", "truststore.p12")
            .withEnv("KAFKA_SSL_TRUSTSTORE_TYPE", "PKCS12")
            .withEnv("KAFKA_SSL_TRUSTSTORE_CREDENTIALS", "credentials")
            // the SSL listener requires the gateway's client certificate; the SASL_SSL one logs it in with SASL
            .withEnv("KAFKA_SSL_CLIENT_AUTH", "required")
            .withEnv("KAFKA_LISTENER_NAME_SASL__SSL_SSL_CLIENT_AUTH", "none")
            .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .withEnv("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
            .withCopyFileToContainer(MountableFile.forHostPath(TestTls.file("broker.p12"), READABLE), "/etc/kafka/secrets/broker.p12")
            .withCopyFileToContainer(MountableFile.forHostPath(TestTls.file("truststore.p12"), READABLE),
                    "/etc/kafka/secrets/truststore.p12")
            .withCopyFileToContainer(MountableFile.forHostPath(credentialsFile(), READABLE), "/etc/kafka/secrets/credentials")
            .withCreateContainerCmdModifier(cmd -> {
                // only exposed container ports get published; cp-kafka does not expose these listeners
                cmd.withExposedPorts(ExposedPort.tcp(SSL_PORT), ExposedPort.tcp(SASL_SSL_PORT));
                cmd.getHostConfig().withPortBindings(
                        new PortBinding(Ports.Binding.bindPort(HOST_SSL_PORT), ExposedPort.tcp(SSL_PORT)),
                        new PortBinding(Ports.Binding.bindPort(HOST_SASL_SSL_PORT), ExposedPort.tcp(SASL_SSL_PORT)));
            })
            .withCommand("bash", "-c",
                    "cat > /etc/kafka/secrets/kafka_server_jaas.conf << 'EOF'\n"
                    + "KafkaServer {\n"
                    + "  org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                    + "  user_" + BROKER_USER + "=\"" + BROKER_PASSWORD + "\";\n"
                    + "};\n"
                    + "EOF\n"
                    + "export KAFKA_OPTS='-Djava.security.auth.login.config=/etc/kafka/secrets/kafka_server_jaas.conf' && "
                    + "/etc/confluent/docker/run")
            .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1))
            .withStartupTimeout(Duration.ofMinutes(2));

    private AdminClient brokerAdmin;

    @BeforeAll
    void setUp() throws Exception {
        kafka.start();
        brokerAdmin = AdminClient.create(mutualTlsProps("localhost:" + HOST_SSL_PORT));
        brokerAdmin.createTopics(List.of(new NewTopic(CONFIG_TOPIC, 1, (short) 1)
                .configs(Map.of("cleanup.policy", "compact")))).all().get(30, TimeUnit.SECONDS);
        writeConfigSnapshot();
    }

    @AfterAll
    void tearDown() {
        if (brokerAdmin != null) {
            brokerAdmin.close();
        }
        kafka.stop();
    }

    @Test
    void reachesTheClusterOverSslWithAClientCertificate() throws Exception {
        // given - PKCS12 stores, and the gateway's certificate for the listener that requires one
        var tls = new TlsConfig(
                store("truststore.p12", StoreType.PKCS12),
                store("gateway.p12", StoreType.PKCS12),
                true);
        var gateway = startGateway("localhost:" + HOST_SSL_PORT, tls, null);

        try {
            // when / then - Kafka traffic through the gateway, and a topic created by its admin API
            assertRoundTrip(gateway, "ssl-orders");
            var created = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + gateway.adminBoundPort() + "/topics"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(
                                    "{\"type\":\"physical\",\"name\":\"ssl-admin\",\"partitions\":1,\"replicationFactor\":1}"))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(created.statusCode())
                    .withFailMessage(() -> "The admin API could not create a topic over TLS: " + created.body())
                    .isEqualTo(201);
            assertThat(brokerAdmin.listTopics().names().get(30, TimeUnit.SECONDS))
                    .withFailMessage(() -> "Topic 'ssl-admin' created through the admin API is not on the broker")
                    .contains("ssl-admin");
        } finally {
            gateway.stop();
        }
    }

    @Test
    void reachesTheClusterOverSaslSslWithAPemTruststore() throws Exception {
        // given - a PEM truststore and a SASL login, no client certificate
        var tls = new TlsConfig(store("ca.pem", StoreType.PEM), null, true);
        var gateway = startGateway("localhost:" + HOST_SASL_SSL_PORT, tls,
                new BrokerAuthConfig("PLAIN", BROKER_USER, BROKER_PASSWORD));

        try {
            // when / then
            assertRoundTrip(gateway, "sasl-ssl-orders");
        } finally {
            gateway.stop();
        }
    }

    @Test
    void failsToStartWhenTheBrokersCertificateIsNotTrusted() {
        // given - a truststore holding an unrelated CA
        var tls = new TlsConfig(store("other-ca.pem", StoreType.PEM), null, true);
        var gateway = new KafkaGateway(bootstrapConfig("localhost:" + HOST_SASL_SSL_PORT, tls,
                new BrokerAuthConfig("PLAIN", BROKER_USER, BROKER_PASSWORD)));

        // when / then - startup fails instead of hanging on the unreachable config topic
        assertThatThrownBy(gateway::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot read config topic");
    }

    private void assertRoundTrip(KafkaGateway gateway, String topic) throws Exception {
        String bootstrap = "localhost:" + gateway.boundPort();
        try (var admin = AdminClient.create(clientProps(bootstrap))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
        var producerProps = clientProps(bootstrap);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(producerProps)) {
            producer.send(new ProducerRecord<>(topic, "key", "over-tls")).get(30, TimeUnit.SECONDS);
        }
        var consumerProps = clientProps(bootstrap);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (var consumer = new KafkaConsumer<String, String>(consumerProps)) {
            var partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            var values = new java.util.ArrayList<String>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (values.isEmpty() && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> values.add(record.value()));
            }
            assertThat(values)
                    .withFailMessage(() -> "The record produced to '" + topic + "' through the gateway was not consumed")
                    .containsExactly("over-tls");
        }
    }

    private KafkaGateway startGateway(String brokerBootstrap, TlsConfig tls, BrokerAuthConfig brokerAuth) throws Exception {
        var gateway = new KafkaGateway(bootstrapConfig(brokerBootstrap, tls, brokerAuth));
        gateway.start();
        return gateway;
    }

    private static GatewayConfig bootstrapConfig(String brokerBootstrap, TlsConfig tls, BrokerAuthConfig brokerAuth) {
        return new GatewayConfig(
                List.of(new ListenerConfig("127.0.0.1", 0)),
                Map.of("default", new ClusterConfig(List.of(brokerBootstrap), tls)),
                null,
                new AdvertisedListener(1, "localhost", 0),
                new AuthConfig(null, null, brokerAuth),
                null,
                new AdminConfig(true, "127.0.0.1", 0, null),
                CONFIG_TOPIC, null);
    }

    /// The gateway's dynamic config: one PLAIN client with access to everything.
    private void writeConfigSnapshot() throws Exception {
        var auth = new AuthConfig(Set.of("PLAIN"),
                Map.of(CLIENT, new ClientConfig("PLAIN", HashedPassword.fromPlaintext(Mechanism.PLAIN, CLIENT_PASSWORD))),
                null);
        var allowAll = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.GROUP, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.CLUSTER, null), AclOperation.ALL)));
        var rbac = new RbacConfig(Map.of("allow-all", allowAll),
                Map.of("clients", new GroupConfig(List.of(CLIENT), List.of("allow-all"))));
        var config = new GatewayConfig(null, null, Map.of(), null, auth, rbac, null, CONFIG_TOPIC, null);
        var props = mutualTlsProps("localhost:" + HOST_SSL_PORT);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(props)) {
            producer.send(new ProducerRecord<>(CONFIG_TOPIC, "config", JsonMapper.builder().build().writeValueAsString(config)))
                    .get(30, TimeUnit.SECONDS);
        }
    }

    /// A client of the gateway itself: plaintext, logged in with SASL PLAIN.
    private static Properties clientProps(String bootstrap) {
        var props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put(SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"" + CLIENT + "\" password=\"" + CLIENT_PASSWORD + "\";");
        return props;
    }

    /// A direct client of the broker's SSL listener, with the gateway's certificate.
    private static Properties mutualTlsProps(String bootstrap) {
        var props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put(SECURITY_PROTOCOL_CONFIG, "SSL");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TestTls.file("truststore.p12").toString());
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TestTls.PASSWORD);
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, TestTls.file("gateway.p12").toString());
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, TestTls.PASSWORD);
        return props;
    }

    private static StoreConfig store(String fixture, StoreType type) {
        return new StoreConfig(TestTls.file(fixture).toString(), type, type == StoreType.PEM ? null : TestTls.PASSWORD, null);
    }

    private static Path credentialsFile() {
        try {
            Path file = Files.createTempFile("kafka-credentials", "");
            file.toFile().deleteOnExit();
            return Files.writeString(file, TestTls.PASSWORD);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static int freePort() {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
