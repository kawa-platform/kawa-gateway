package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.ListenerConfig;
import io.jonasg.kawa.config.Mechanism;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.server.KafkaGateway;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/// Shared lifecycle for integration tests: one Testcontainers Kafka broker, an in-JVM
/// [KafkaGateway] in front of it, and admin/producer/consumer clients on both sides.
///
/// The gateway's dynamic config (virtual topics, RBAC, client auth) is written to the
/// `__kawa` config topic before the gateway starts; the gateway is given the static bootstrap
/// (cluster bootstrap servers, config topic name, listeners, advertised, admin).
///
/// Subclasses override [virtualTopics] and [initialTopics] to describe the
/// gateway virtual-topic map and the physical topics to pre-create. Fields are `protected static`
/// so tests can use them directly.
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class GatewayTestSupport {

    /// The config topic the gateway reads its dynamic config from.
    protected static final String CONFIG_TOPIC = "__kawa";

    /// The canonical principal the shared gateway clients authenticate as, and the password
    /// they use. `authConfig()` grants this user broad access via `rbacConfig()`, so tests that
    /// use `gatewayProducer`/`gatewayConsumer`/`gatewayAdmin` work without configuring their own
    /// RBAC. Subclasses that override `authConfig()` and still want to use the shared clients
    /// must include this principal (and password) in their own user map, or grant equivalent
    /// RBAC access to whatever principal they do define.
    protected static final String DEFAULT_PRINCIPAL = "gateway-it";
    protected static final String DEFAULT_PASSWORD = "gateway-it-secret";

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))
            .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
            .withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true")
            .withEnv("KAFKA_SUPER_USERS", "User:ANONYMOUS");

    protected static KafkaGateway gateway;
    protected static String gatewayBootstrap;
    protected static String brokerBootstrap;

    protected static KafkaProducer<String, String> gatewayProducer;
    protected static KafkaConsumer<String, String> brokerConsumer;
    protected static KafkaConsumer<String, String> gatewayConsumer;
    protected static AdminClient brokerAdmin;
    protected static AdminClient gatewayAdmin;

    @BeforeAll
    void setUp() throws Exception {
        brokerBootstrap = kafka.getBootstrapServers();

        brokerAdmin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap));
        brokerAdmin.createTopics(List.of(new NewTopic(CONFIG_TOPIC, 1, (short) 1)
                .configs(Map.of("cleanup.policy", "compact")))).all().get();
        brokerAdmin.createTopics(initialTopics()).all().get();
        writeConfigSnapshot();

        gateway = new KafkaGateway(buildBootstrap());
        gateway.start();
        gatewayBootstrap = "localhost:" + gateway.boundPort();

        gatewayAdmin = AdminClient.create(saslProps(gatewayBootstrap));

        Properties producerProps = saslProps(gatewayBootstrap);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        gatewayProducer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap);
        consumerProps.put(GROUP_ID_CONFIG, groupId());
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        brokerConsumer = new KafkaConsumer<>(consumerProps);

        consumerProps = saslProps(gatewayBootstrap);
        consumerProps.put(GROUP_ID_CONFIG, groupId());
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        gatewayConsumer = new KafkaConsumer<>(consumerProps);
    }

    /// A `Properties` map pointed at `bootstrap` that authenticates to the gateway as
    /// [DEFAULT_PRINCIPAL] over SASL_PLAINTEXT/PLAIN. Used for every client that talks to the
    /// gateway, since RBAC is always enforced and an unauthenticated client is denied.
    /// Subclasses building their own gateway clients should start from this map.
    protected static Properties saslProps(String bootstrap) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"" + DEFAULT_PRINCIPAL + "\" password=\"" + DEFAULT_PASSWORD + "\";");
        return props;
    }

    /// A `Properties` map pointed at `bootstrap` that authenticates to the gateway as
    /// `username`/`password` over SASL_PLAINTEXT with the given SCRAM mechanism, using the
    /// standard `ScramLoginModule` a real client would configure.
    protected static Properties scramSaslProps(String bootstrap, String mechanism, String username, String password) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, mechanism);
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username
                + "\" password=\"" + password + "\";");
        return props;
    }

    @AfterAll
    void tearDown() {
        try {
            if (gatewayProducer != null) {
                gatewayProducer.close();
            }
            if (brokerConsumer != null) {
                brokerConsumer.close();
            }
            if (brokerAdmin != null) {
                brokerAdmin.close();
            }
            if (gatewayAdmin != null) {
                gatewayAdmin.close();
            }
            if (gateway != null) {
                gateway.stop();
            }
        } finally {
            if (kafka != null) {
                kafka.stop();
            }
        }
    }

    protected Map<String, String> virtualTopics() {
        return Map.of();
    }

    protected Map<String, VirtualTopicConfig> filteredVirtualTopics() {
        return Map.of();
    }

    protected List<NewTopic> initialTopics() {
        return List.of();
    }

    protected String groupId() {
        return "gateway-it";
    }

    /// The gateway's client SASL configuration. Defaults to a single PLAIN user,
    /// [DEFAULT_PRINCIPAL], so the shared clients can authenticate. Subclasses that override
    /// this and still use the shared clients must include [DEFAULT_PRINCIPAL]/[DEFAULT_PASSWORD]
    /// in their user map (or grant equivalent RBAC access to whatever principal they define).
    protected AuthConfig authConfig() {
        return new AuthConfig(
                Set.of("PLAIN"),
                Map.of(DEFAULT_PRINCIPAL, client("PLAIN", DEFAULT_PASSWORD)),
                null);
    }

    protected static ClientConfig client(String mechanism, String password) {
        return new ClientConfig(mechanism,
                HashedPassword.fromPlaintext(Mechanism.fromWireName(mechanism), password));
    }

    /// The gateway's RBAC configuration. Defaults to granting [DEFAULT_PRINCIPAL] broad access
    /// via a wildcard role, so the shared clients pass through real (broad) RBAC rather than
    /// being denied. Subclasses that override `authConfig()` with a different principal must
    /// grant that principal equivalent access here.
    protected RbacConfig rbacConfig() {
        var allowAllRole = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.GROUP, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.TRANSACTIONAL_ID, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.CLUSTER, null), AclOperation.ALL)));
        return new RbacConfig(
                Map.of("allow-all", allowAllRole),
                Map.of("it-defaults", new GroupConfig(List.of(DEFAULT_PRINCIPAL), List.of("allow-all"))));
    }

    /// The gateway's admin HTTP server configuration.
    protected AdminConfig adminConfig() {
        return new AdminConfig(true, "127.0.0.1", 0, null);
    }

    /// The static bootstrap passed to the gateway: cluster bootstrap servers, config topic
    /// name, listeners, advertised endpoint and admin. Virtual topics, RBAC and client auth
    /// come from the config topic snapshot.
    private GatewayConfig buildBootstrap() {
        return new GatewayConfig(
                List.of(new ListenerConfig("127.0.0.1", 0)),
                Map.of("default", new ClusterConfig(List.of(brokerBootstrap))),
                null,
                new AdvertisedListener(1, "localhost", 0),
                new AuthConfig(null, null, null),
                null,
                adminConfig(),
                CONFIG_TOPIC, null);
    }

    /// The full dynamic config written to the config topic before the gateway starts. Only
    /// virtual topics, RBAC and client auth are dynamic; listeners, advertised and admin are
    /// startup-only and come from the static bootstrap.
    private GatewayConfig buildDynamicConfig() {
        Map<String, VirtualTopicConfig> typedVirtualTopics = new java.util.HashMap<>();
        virtualTopics().forEach((virtual, physical) ->
                typedVirtualTopics.put(virtual, new VirtualTopicConfig(physical)));
        typedVirtualTopics.putAll(filteredVirtualTopics());
        return new GatewayConfig(
                null,
                Map.of("default", new ClusterConfig(List.of(brokerBootstrap))),
                typedVirtualTopics,
                null,
                authConfig(),
                rbacConfig(),
                null,
                CONFIG_TOPIC, null);
    }

    /// Serializes the full dynamic config as JSON and writes it to the config topic, so the
    /// gateway's initial catch-up finds it at boot.
    private void writeConfigSnapshot() throws Exception {
        var mapper = JsonMapper.builder().build();
        String json = mapper.writeValueAsString(buildDynamicConfig());
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap);
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(props)) {
            producer.send(new ProducerRecord<>(CONFIG_TOPIC, "test-gateway", json)).get();
        }
    }
}
