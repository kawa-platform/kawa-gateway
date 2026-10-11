package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.AuthConfig;
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
import io.jonasg.kawa.server.KafkaGateway;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/// Consumer groups and transactions through the gateway in front of a three-broker cluster, with
/// group and transactional ids whose coordinator is not the lowest broker id: every group- and
/// transaction-coordinator request must reach that coordinator, or the broker answers
/// NOT_COORDINATOR and the group never stabilises (issue #55). A single-broker cluster cannot show
/// this, since its only broker coordinates everything.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConsumerGroupMultiBrokerIT {

    private static final String CONFIG_TOPIC = "__kawa";
    private static final String TOPIC = "orders";
    private static final int PARTITIONS = 6;
    private static final String CLIENT = "client";
    private static final String CLIENT_PASSWORD = "client-secret";

    private final MultiBrokerKafkaCluster cluster = new MultiBrokerKafkaCluster(3);
    private AdminClient brokerAdmin;
    private KafkaGateway gateway;
    private String gatewayBootstrap;

    @BeforeAll
    void setUp() throws Exception {
        cluster.start();
        brokerAdmin = AdminClient.create(Map.of("bootstrap.servers", cluster.bootstrapServers()));
        brokerAdmin.createTopics(List.of(
                new NewTopic(CONFIG_TOPIC, 1, (short) 1).configs(Map.of("cleanup.policy", "compact")),
                new NewTopic(TOPIC, PARTITIONS, (short) 1))).all().get(30, TimeUnit.SECONDS);
        createCoordinatorTopics();
        writeConfigSnapshot();

        gateway = new KafkaGateway(new GatewayConfig(
                List.of(new ListenerConfig("127.0.0.1", 0)),
                Map.of("default", new ClusterConfig(List.of(cluster.bootstrapServers().split(",")))),
                null,
                new AdvertisedListener(1, "localhost", 0),
                new AuthConfig(null, null, null),
                null,
                null,
                CONFIG_TOPIC, null));
        gateway.start();
        gatewayBootstrap = "localhost:" + gateway.boundPort();
    }

    @AfterAll
    void tearDown() {
        if (gateway != null) {
            gateway.stop();
        }
        if (brokerAdmin != null) {
            brokerAdmin.close();
        }
        cluster.close();
    }

    @Test
    @Disabled("A broker handles one request per connection at a time, and the gateway shares one connection per broker"
            + " across all clients: a JoinGroup the coordinator holds for the rebalance blocks the other member's"
            + " heartbeats and rejoin behind it. Needs a broker connection per client connection.")
    void rebalancesAndCommitsAConsumerGroupCoordinatedByAnotherBroker() throws Exception {
        // given
        String group = groupNotCoordinatedByTheLowestBroker("orders-readers");
        produce(PARTITIONS);

        try (var first = consumer(group); var second = consumer(group)) {
            first.subscribe(List.of(TOPIC));
            second.subscribe(List.of(TOPIC));

            // when - both members poll until the group has rebalanced over them, read every record
            // and committed; a commit made while the group is still settling is retried
            List<ConsumerRecord<String, String>> consumed = new ArrayList<>();
            Awaitility.await("group '" + group + "' to rebalance over both members, read every record and commit")
                    .atMost(Duration.ofSeconds(60))
                    .until(() -> {
                        first.poll(Duration.ofMillis(200)).forEach(consumed::add);
                        second.poll(Duration.ofMillis(200)).forEach(consumed::add);
                        return !first.assignment().isEmpty() && !second.assignment().isEmpty()
                                && consumed.size() >= PARTITIONS
                                && commits(first) && commits(second);
                    });

            // then
            var committed = brokerAdmin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                    .get(30, TimeUnit.SECONDS);
            assertThat(committed.keySet())
                    .withFailMessage(() -> "Group '" + group + "' did not commit every partition: " + committed)
                    .hasSize(PARTITIONS);
        }
    }

    private static boolean commits(KafkaConsumer<String, String> consumer) {
        try {
            consumer.commitSync(Duration.ofSeconds(5));
            return true;
        } catch (CommitFailedException | RebalanceInProgressException e) {
            return false;
        }
    }

    @Test
    void commitsATransactionCoordinatedByAnotherBroker() throws Exception {
        // given
        String transactionalId = transactionalIdNotCoordinatedByTheLowestBroker();
        String group = groupNotCoordinatedByTheLowestBroker("txn-readers");
        var props = clientProps();
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (var producer = new KafkaProducer<String, String>(props)) {
            // when - InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit and EndTxn
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(TOPIC, 0, "txn", "committed")).get(30, TimeUnit.SECONDS);
            producer.sendOffsetsToTransaction(Map.of(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(1)),
                    new ConsumerGroupMetadata(group));
            producer.commitTransaction();
        }

        // then
        var committed = brokerAdmin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS);
        assertThat(committed.get(new TopicPartition(TOPIC, 0)))
                .withFailMessage(() -> "The transaction's offsets for group '" + group + "' were not committed: " + committed)
                .isNotNull();
    }

    /// Describing a group and initializing a transaction create `__consumer_offsets` and
    /// `__transaction_state`, so the gateway's first metadata load already knows the coordinators.
    private void createCoordinatorTopics() throws Exception {
        brokerAdmin.describeConsumerGroups(List.of("warm-up")).all().get(30, TimeUnit.SECONDS);
        var props = new Properties();
        props.put("bootstrap.servers", cluster.bootstrapServers());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "warm-up");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(props)) {
            producer.initTransactions();
        }
    }

    /// The first `<prefix>-<n>` group the brokers themselves coordinate on a broker other than 1.
    private String groupNotCoordinatedByTheLowestBroker(String prefix) {
        return IntStream.range(0, 100).mapToObj(n -> prefix + "-" + n)
                .filter(group -> coordinatorOf(group) != 1)
                .findFirst().orElseThrow();
    }

    private int coordinatorOf(String group) {
        try {
            return brokerAdmin.describeConsumerGroups(List.of(group)).all().get(30, TimeUnit.SECONDS)
                    .get(group).coordinator().id();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /// The first `txn-<n>` id whose `__transaction_state` partition is led by a broker other than 1,
    /// with Kafka's own mapping (`TransactionStateManager.partitionFor`).
    private String transactionalIdNotCoordinatedByTheLowestBroker() throws Exception {
        List<TopicPartitionInfo> partitions = brokerAdmin.describeTopics(List.of("__transaction_state"))
                .allTopicNames().get(30, TimeUnit.SECONDS).get("__transaction_state").partitions();
        return IntStream.range(0, 100).mapToObj(n -> "txn-" + n)
                .filter(id -> {
                    Node leader = partitions.get(Utils.abs(id.hashCode()) % partitions.size()).leader();
                    return leader.id() != 1;
                })
                .findFirst().orElseThrow();
    }

    private void produce(int records) throws Exception {
        var props = clientProps();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(props)) {
            for (int partition = 0; partition < records; partition++) {
                producer.send(new ProducerRecord<>(TOPIC, partition % PARTITIONS, "key", "value-" + partition))
                        .get(30, TimeUnit.SECONDS);
            }
        }
    }

    private KafkaConsumer<String, String> consumer(String group) {
        var props = clientProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    private Properties clientProps() {
        var props = new Properties();
        props.put("bootstrap.servers", gatewayBootstrap);
        props.put(SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"" + CLIENT + "\" password=\"" + CLIENT_PASSWORD + "\";");
        return props;
    }

    /// The gateway's dynamic config: one PLAIN client with access to everything.
    private void writeConfigSnapshot() throws Exception {
        var auth = new AuthConfig(Set.of("PLAIN"),
                Map.of(CLIENT, new ClientConfig("PLAIN", HashedPassword.fromPlaintext(Mechanism.PLAIN, CLIENT_PASSWORD))),
                null);
        var allowAll = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.GROUP, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.TRANSACTIONAL_ID, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.CLUSTER, null), AclOperation.ALL)));
        var rbac = new RbacConfig(Map.of("allow-all", allowAll),
                Map.of("clients", new GroupConfig(List.of(CLIENT), List.of("allow-all"))));
        var config = new GatewayConfig(null, null, Map.of(), null, auth, rbac, null, CONFIG_TOPIC, null);
        var props = new Properties();
        props.put("bootstrap.servers", cluster.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(props)) {
            producer.send(new ProducerRecord<>(CONFIG_TOPIC, "config", JsonMapper.builder().build().writeValueAsString(config)))
                    .get(30, TimeUnit.SECONDS);
        }
    }
}
