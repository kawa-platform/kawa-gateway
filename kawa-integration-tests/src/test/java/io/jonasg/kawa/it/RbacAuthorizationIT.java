package io.jonasg.kawa.it;

import io.jonasg.kassert.Kassertions;
import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Real-client RBAC coverage: drives actual Kafka clients (producer, consumer, admin) through
/// the gateway against a real broker, one representative scenario per authorization gate.
/// Five principals map to five roles via five groups; `orders` is the only topic and
/// `orders-group` the only group.
class RbacAuthorizationIT extends GatewayTestSupport {

    private static final String PASSWORD = "secret";

    private final List<KafkaConsumer<String, String>> consumers = new ArrayList<>();
    private final List<KafkaProducer<String, String>> producers = new ArrayList<>();
    private final List<AdminClient> admins = new ArrayList<>();

    @Override
    protected List<NewTopic> initialTopics() {
        return List.of(new NewTopic("orders", 1, (short) 1));
    }

    @Override
    protected AuthConfig authConfig() {
        return new AuthConfig(
                Set.of("PLAIN"),
                Map.of(
                        "alice", client("PLAIN", PASSWORD),
                        "bob", client("PLAIN", PASSWORD),
                        "carol", client("PLAIN", PASSWORD),
                        "dave", client("PLAIN", PASSWORD),
                        "erin", client("PLAIN", PASSWORD),
                        // GatewayTestSupport builds shared gatewayProducer/gatewayConsumer/gatewayAdmin
                        // as DEFAULT_PRINCIPAL for every subclass. This class never uses them, but if
                        // that principal cannot authenticate they reconnect in a tight loop for the
                        // lifetime of the class, flooding the log and starving the tests that matter.
                        DEFAULT_PRINCIPAL, client("PLAIN", DEFAULT_PASSWORD)),
                null);
    }

    @Override
    protected RbacConfig rbacConfig() {
        var producer = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "orders"), AclOperation.WRITE),
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "orders"), AclOperation.DESCRIBE)));
        var consumer = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "orders"), AclOperation.READ),
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "orders"), AclOperation.DESCRIBE)));
        var describeOnly = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "orders"), AclOperation.DESCRIBE)));
        var groupMember = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.GROUP, "orders-group"), AclOperation.READ),
                new AclConfig(new ResourceConfig(ResourceType.GROUP, "orders-group"), AclOperation.DESCRIBE)));
        var admin = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.CLUSTER, null), AclOperation.CREATE),
                new AclConfig(new ResourceConfig(ResourceType.CLUSTER, null), AclOperation.DELETE),
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.GROUP, "", PatternType.PREFIXED), AclOperation.ALL)));
        return new RbacConfig(
                Map.of(
                        "producer", producer,
                        "consumer", consumer,
                        "describe-only", describeOnly,
                        "group-member", groupMember,
                        "admin", admin),
                Map.of(
                        "full-access", new GroupConfig(List.of("alice"), List.of("producer", "consumer", "group-member")),
                        "group-only", new GroupConfig(List.of("bob"), List.of("group-member")),
                        "cluster-admins", new GroupConfig(List.of("carol"), List.of("admin")),
                        "no-access", new GroupConfig(List.of("dave"), List.of()),
                        "visibility-only", new GroupConfig(List.of("erin"), List.of("describe-only", "group-member"))));
    }

    @AfterEach
    void closeClients() {
        consumers.forEach(KafkaConsumer::close);
        producers.forEach(KafkaProducer::close);
        admins.forEach(AdminClient::close);
        consumers.clear();
        producers.clear();
        admins.clear();
    }

    @Test
    void producerWithTopicWriteAllowedCanProduce() throws Exception {
        // given
        KafkaProducer<String, String> producer = newProducer("alice");

        // when
        RecordMetadata metadata = producer.send(new ProducerRecord<>("orders", "k1", "v1")).get(5, TimeUnit.SECONDS);

        // then
        assertThat(metadata.topic()).isEqualTo("orders");
        assertThat(metadata.partition()).isZero();
    }

    @Test
    void producerWithoutTopicWriteIsDeniedWithTopicAuthorizationException() {
        // given
        KafkaProducer<String, String> producer = newProducer("erin");

        // when
        Future<RecordMetadata> send = producer.send(new ProducerRecord<>("orders", "k1", "v1"));

        // then
        assertThatThrownBy(() -> send.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TopicAuthorizationException.class);
    }

    @Test
    void consumerWithDescribeAndReadCanConsume() {
        // given
        KafkaProducer<String, String> producer = newProducer("alice");
        produce(producer, "orders", "k1", "v1");

        // when
        var consume = Kassertions.consume(newConsumer("alice", "orders-group"))
                .assignedTo("orders", 0)
                .fromBeginning()
                .within(10, TimeUnit.SECONDS);

        // then
        consume.anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
    }

    @Test
    void consumerGroupWithGroupAccessAllowedCanJoin() {
        // given
        KafkaProducer<String, String> producer = newProducer("alice");
        produce(producer, "orders", "k1", "v1");
        KafkaConsumer<String, String> consumer = newConsumer("alice", "orders-group");
        consumer.subscribe(List.of("orders"));

        // when
        ConsumerRecord<String, String> record = pollForValue(consumer, "v1");

        // then
        assertThat(record).isNotNull();
        assertThat(record.value()).isEqualTo("v1");
    }

    @Test
    void consumerGroupWithoutGroupAccessIsDeniedWithGroupAuthorizationException() {
        // given
        KafkaConsumer<String, String> consumer = newConsumer("dave", "orders-group");
        consumer.subscribe(List.of("orders"));

        // when/then
        assertThatThrownBy(() -> consumer.poll(Duration.ofSeconds(10)))
                .isInstanceOf(GroupAuthorizationException.class);
    }

    @Test
    void adminWithClusterAllAllowedCanCreateAndDeleteTopics() throws Exception {
        // given
        AdminClient admin = newAdmin("carol");
        String topic = "carol-topic";

        // when
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);

        // then
        assertThat(admin.listTopics().names().get(10, TimeUnit.SECONDS)).contains(topic);

        // when
        admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);

        // then
        assertThat(admin.listTopics().names().get(10, TimeUnit.SECONDS)).doesNotContain(topic);
    }

    @Test
    void adminWithoutClusterAccessIsDeniedWithClusterAuthorizationException() {
        // given
        AdminClient admin = newAdmin("dave");

        // when/then
        assertThatThrownBy(() -> admin.createTopics(List.of(new NewTopic("dave-topic", 1, (short) 1))).all()
                .get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ClusterAuthorizationException.class);
    }

    @Test
    void consumerWithDescribeButNoReadGetsTopicAuthorizationExceptionOnFetch() {
        // given
        KafkaConsumer<String, String> consumer = newConsumer("erin", "orders-group");
        consumer.subscribe(List.of("orders"));

        // when
        // the first poll joins the group (erin has GROUP access) and then fetches, which is denied
        assertThatThrownBy(() -> consumer.poll(Duration.ofSeconds(15)))
                .isInstanceOf(TopicAuthorizationException.class);

        // then
        // observed: subsequent polls keep throwing while the unauthorized topic stays assigned
        List<Boolean> subsequentPolls = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            try {
                consumer.poll(Duration.ofSeconds(2));
                subsequentPolls.add(false);
            } catch (TopicAuthorizationException e) {
                subsequentPolls.add(true);
            }
        }
        assertThat(subsequentPolls).allMatch(threw -> threw);
    }

    @Test
    @Timeout(30)
    void principalWithoutDescribeCannotSeeTopicAtAll() throws Exception {
        // given
        AdminClient admin = newAdmin("bob");

        // when
        Set<String> visible = admin.listTopics().names().get(10, TimeUnit.SECONDS);

        // then
        // bob holds no TOPIC ACLs, so MetadataAuthorizationCheck.onResponse strips `orders` from
        // the list-all response: the topic is not merely unreadable, it is invisible.
        //
        // This deliberately does not go through subscribe()+poll(). A consumer whose only
        // subscribed topic is invisible becomes the group leader, can never resolve the
        // subscription to a partition, and so never sends SyncGroup - poll() then blocks inside
        // the rebalance instead of honouring its timeout, which is why the previous assertion
        // could not pass.
        assertThat(visible).doesNotContain("orders");
    }

    @Test
    @Timeout(10)
    void unauthenticatedProducerIsDeniedWithSaslAuthenticationFailed() throws Exception {
        // when
        ProduceResponseData response = produceWithoutAuthenticating("orders", 0, "k1", "v1");

        // then
        assertThat(response.responses()).hasSize(1);
        ProduceResponseData.PartitionProduceResponse partition =
                response.responses().iterator().next().partitionResponses().get(0);
        assertThat(partition.errorCode()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED.code());
    }

    private static Properties saslPropsFor(String username) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, gatewayBootstrap);
        props.put(SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"" + username + "\" password=\"" + PASSWORD + "\";");
        return props;
    }

    private static final short PRODUCE_VERSION = 8;

    /// Sends a single Produce request over a raw socket with no SASL handshake at all — the
    /// scenario `ProduceAuthorizationCheck`'s `principal == null` branch exists to deny. Opens
    /// one connection, writes one request, reads one response, closes.
    private static ProduceResponseData produceWithoutAuthenticating(
            String topic,
            int partition,
            String key,
            String value
    ) throws IOException {
        String[] parts = gatewayBootstrap.split(":");
        int port = Integer.parseInt(parts[1]);
        Socket chosen = null;
        for (InetAddress address : InetAddress.getAllByName(parts[0])) {
            try {
                chosen = new Socket();
                chosen.connect(new InetSocketAddress(address, port), 5000);
                break;
            } catch (IOException ignored) {
            }
        }
        if (chosen == null) {
            throw new IOException("could not connect to " + gatewayBootstrap);
        }
        try (Socket socket = chosen) {
            socket.setSoTimeout(10_000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            var data = new ProduceRequestData().setAcks((short) 1).setTimeoutMs(30_000);
            var records = MemoryRecords.withRecords(Compression.NONE,
                    new SimpleRecord(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8)));
            data.topicData().add(new ProduceRequestData.TopicProduceData().setName(topic)
                    .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()
                            .setIndex(partition)
                            .setRecords(records))));

            RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, PRODUCE_VERSION, "kawa-raw-it", 1);
            ProduceRequest request = new ProduceRequest.Builder(PRODUCE_VERSION, PRODUCE_VERSION, data)
                    .build(PRODUCE_VERSION);
            ByteBuffer payload = request.serializeWithHeader(header);
            out.writeInt(payload.remaining());
            out.write(payload.array(), payload.position(), payload.remaining());
            out.flush();

            int size = in.readInt();
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            short responseHeaderVersion = ApiKeys.PRODUCE.responseHeaderVersion(PRODUCE_VERSION);
            ResponseHeader.parse(buffer, responseHeaderVersion);
            return ProduceResponse.parse(new ByteBufferAccessor(buffer), PRODUCE_VERSION).data();
        }
    }

    private KafkaProducer<String, String> newProducer(String username) {
        Properties props = saslPropsFor(username);
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producers.add(producer);
        return producer;
    }

    private KafkaConsumer<String, String> newConsumer(String username, String groupId) {
        Properties props = saslPropsFor(username);
        props.put(GROUP_ID_CONFIG, groupId);
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumers.add(consumer);
        return consumer;
    }

    private AdminClient newAdmin(String username) {
        AdminClient admin = AdminClient.create(saslPropsFor(username));
        admins.add(admin);
        return admin;
    }

    private void produce(KafkaProducer<String, String> producer, String topic, String key, String value) {
        try {
            producer.send(new ProducerRecord<>(topic, key, value)).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private ConsumerRecord<String, String> pollForValue(KafkaConsumer<String, String> consumer, String value) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(2))) {
                if (value.equals(record.value())) {
                    return record;
                }
            }
        }
        return null;
    }
}
