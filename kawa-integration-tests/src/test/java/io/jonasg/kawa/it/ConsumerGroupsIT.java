package io.jonasg.kawa.it;

import io.jonasg.kawa.broker.KafkaBrokerAdmin;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.http.consumergroups.ConsumerGroupService;
import io.jonasg.kawa.http.consumergroups.ConsumerGroupService.ConsumerGroupDetail;
import io.jonasg.kawa.http.consumergroups.ConsumerGroupService.ConsumerGroupSummary;
import io.jonasg.kawa.http.consumergroups.ConsumerGroupService.PartitionLag;
import io.jonasg.kawa.http.consumergroups.ConsumerGroupService.TopicLag;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.tuple;

/// Consumer group listing and lag against a real broker, read by [ConsumerGroupService] through a
/// [KafkaBrokerAdmin] on the broker. Consumers join through the gateway, so their commits and
/// assignments go through the virtual topic mapping like a real client's; the service reads the
/// physical topics on the broker. The gateway keeps its services private, so the service is built here
/// with the same virtual topic configuration the gateway starts with.
///
/// Every test uses a fresh group id. The fixtures are produced once: [#RECORDS] records to `orders`
/// (unfiltered, `orders-v2`) and to `orders-acme` (filtered, `orders-acme-v2`).
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConsumerGroupsIT extends GatewayTestSupport {

    private static final int RECORDS = 10;
    private static final long COMMITTED = 4;
    private static final long LAG = RECORDS - COMMITTED;
    private static final String ORDERS = "orders";
    private static final String ORDERS_ACME = "orders-acme";
    private static final String ORDERS_V2 = "orders-v2";
    private static final String ORDERS_ACME_V2 = "orders-acme-v2";

    // Share groups are on by default in Kafka 4.3, but their coordinator keeps state in `__share_group_state`, which
    // defaults to replication factor 3 and min ISR 2. Testcontainers lowers these for the offsets and transaction
    // topics only, so on its single broker a share consumer never gets records. The broker starts after this runs.
    static {
        kafka.withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1");
    }

    private ConsumerGroupService groups;
    private KafkaBrokerAdmin brokerAdmin;

    @Override
    protected Map<String, String> virtualTopics() {
        return Map.of(ORDERS, ORDERS_V2);
    }

    @Override
    protected Map<String, VirtualTopicConfig> filteredVirtualTopics() {
        return Map.of(ORDERS_ACME, new VirtualTopicConfig(ORDERS_ACME_V2,
                new HeaderEqualsFilterConfig("tenant", "acme")));
    }

    @Override
    protected List<NewTopic> initialTopics() {
        return List.of(new NewTopic(ORDERS_V2, 1, (short) 1), new NewTopic(ORDERS_ACME_V2, 1, (short) 1));
    }

    @BeforeAll
    void setUpFixtures() throws Exception {
        brokerAdmin = new KafkaBrokerAdmin(brokerBootstrap, new Properties());
        groups = new ConsumerGroupService(virtualTopicManager(), brokerAdmin);
        for (int i = 0; i < RECORDS; i++) {
            gatewayProducer.send(new ProducerRecord<>(ORDERS, null, "key", "value-" + i, tenantAcme())).get();
            gatewayProducer.send(new ProducerRecord<>(ORDERS_ACME, null, "key", "value-" + i, tenantAcme())).get();
        }
    }

    @AfterAll
    void closeBrokerAdmin() {
        if (brokerAdmin != null) {
            brokerAdmin.close();
        }
    }

    @Test
    void reportsLagOnThePhysicalTopicBehindTheVirtualTopic() throws Exception {
        // given
        String group = freshGroup();

        try (var consumer = joinThroughGateway(group, ORDERS)) {
            consumer.commitSync(Map.of(new TopicPartition(ORDERS, 0), new OffsetAndMetadata(COMMITTED)));

            // when / then
            eventually(() -> {
                ConsumerGroupDetail detail = groups.describe(group);
                TopicLag topic = detail.topics().getFirst();
                assertThat(topic.physicalTopic())
                        .withFailMessage(() -> "Lag was not reported against the physical topic: " + topic)
                        .isEqualTo(ORDERS_V2);
                assertThat(topic.virtualTopics())
                        .withFailMessage(() -> "The virtual topic 'orders' was not named on 'orders-v2': " + topic)
                        .containsExactly(ORDERS);
                assertThat(topic.filtered())
                        .withFailMessage(() -> "Unfiltered 'orders' was marked as an upper bound: " + topic)
                        .isFalse();
                assertThat(partition0(topic))
                        .withFailMessage(() -> "Partition 0 did not show committed 4, end 10 and lag 6: " + topic)
                        .isEqualTo(new PartitionLag(0, COMMITTED, (long) RECORDS, LAG));
                assertThat(detail.totalLag())
                        .withFailMessage(() -> "Total lag of group '" + group + "' was not 6: " + detail)
                        .isEqualTo(LAG);
            });
        }
    }

    @Test
    void marksLagAsAnUpperBoundWhenTheVirtualTopicFilters() throws Exception {
        // given
        String group = freshGroup();

        try (var consumer = joinThroughGateway(group, ORDERS_ACME)) {
            consumer.commitSync(Map.of(new TopicPartition(ORDERS_ACME, 0), new OffsetAndMetadata(COMMITTED)));

            // when / then
            eventually(() -> {
                ConsumerGroupDetail detail = groups.describe(group);
                TopicLag topic = detail.topics().getFirst();
                assertThat(topic.physicalTopic())
                        .withFailMessage(() -> "Lag was not reported against the physical topic: " + topic)
                        .isEqualTo(ORDERS_ACME_V2);
                assertThat(topic.virtualTopics())
                        .withFailMessage(() -> "The filtered virtual topic 'orders-acme' was not named: " + topic)
                        .containsExactly(ORDERS_ACME);
                assertThat(topic.filtered())
                        .withFailMessage(() -> "Lag through a filtered virtual topic was not an upper bound: " + topic)
                        .isTrue();
                assertThat(detail.totalLag())
                        .withFailMessage(() -> "Total lag of group '" + group + "' was not 6: " + detail)
                        .isEqualTo(LAG);
            });
        }
    }

    @Test
    void showsAnAssignedPartitionWithoutACommitAsUncommitted() throws Exception {
        // given
        String group = freshGroup();

        try (var consumer = joinThroughGateway(group, ORDERS)) {
            // when / then
            eventually(() -> {
                ConsumerGroupDetail detail = groups.describe(group);
                assertThat(partition0(detail.topics().getFirst()))
                        .withFailMessage(() -> "Assigned partition with no commit was not shown without a committed offset or lag: " + detail)
                        .isEqualTo(new PartitionLag(0, null, (long) RECORDS, null));
                assertThat(detail.uncommittedPartitions())
                        .withFailMessage(() -> "The assigned partition without a commit was not counted: " + detail)
                        .isEqualTo(1);
                assertThat(detail.totalLag())
                        .withFailMessage(() -> "Total lag of group '" + group + "' counted a partition without a commit: " + detail)
                        .isZero();
            });
        }
    }

    @Test
    void keepsShowingTheLagOfAGroupAfterItsConsumerLeaves() throws Exception {
        // given
        String group = freshGroup();
        try (var consumer = joinThroughGateway(group, ORDERS)) {
            consumer.commitSync(Map.of(new TopicPartition(ORDERS, 0), new OffsetAndMetadata(COMMITTED)));
        }

        // when / then
        eventually(() -> {
            ConsumerGroupDetail detail = groups.describe(group);
            assertThat(detail.state())
                    .withFailMessage(() -> "Group '" + group + "' did not turn Empty after its consumer closed: " + detail)
                    .isEqualTo("Empty");
            assertThat(detail.members())
                    .withFailMessage(() -> "Group '" + group + "' still lists a member after its consumer closed: " + detail)
                    .isEmpty();
            assertThat(detail.totalLag())
                    .withFailMessage(() -> "Empty group '" + group + "' lost its lag: " + detail)
                    .isEqualTo(LAG);
        });
    }

    @Test
    void failsToDescribeAGroupTheBrokerDoesNotKnowNamingIt() {
        // when
        var thrown = catchThrowable(() -> groups.describe("no-such-group"));

        // then
        assertThat(thrown)
                .withFailMessage(() -> "Describing an unknown group did not fail naming it: " + thrown)
                .isInstanceOf(NotFoundException.class)
                .hasMessageContaining("no-such-group");
    }

    @Test
    void listsClassicGroupsAsClassicWithTheirLagAndKip848GroupsAsConsumer() throws Exception {
        // given: a classic group through the gateway, with a commit and its consumer still alive
        String classic = freshGroup();
        try (var consumer = joinThroughGateway(classic, ORDERS)) {
            consumer.commitSync(Map.of(new TopicPartition(ORDERS, 0), new OffsetAndMetadata(COMMITTED)));

            // and a KIP-848 group started directly on the broker
            String consumerProtocol = freshGroup();
            try (var kip848 = joinKip848OnBroker(consumerProtocol)) {
                // when / then
                eventually(() -> {
                    List<ConsumerGroupSummary> listed = groups.list();
                    assertThat(listed)
                            .withFailMessage(() -> "The listing did not show classic group '" + classic + "' as CLASSIC with lag 6: " + listed)
                            .filteredOn(group -> group.groupId().equals(classic))
                            .extracting(ConsumerGroupSummary::type, ConsumerGroupSummary::totalLag)
                            .containsExactly(tuple("CLASSIC", LAG));
                    assertThat(listed)
                            .withFailMessage(() -> "The listing did not show KIP-848 group '" + consumerProtocol + "' as CONSUMER: " + listed)
                            .filteredOn(group -> group.groupId().equals(consumerProtocol))
                            .extracting(ConsumerGroupSummary::type)
                            .containsExactly("CONSUMER");
                });
            }
        }
    }

    @Test
    void listsAShareGroupWithLagNotApplicable() throws Exception {
        // given
        String share = freshGroup();

        try (var consumer = joinShareGroupOnBroker(share)) {
            // when / then
            eventually(() -> {
                List<ConsumerGroupSummary> listed = groups.list();
                assertThat(listed)
                        .withFailMessage(() -> "The listing did not show share group '" + share + "' as a Stable SHARE group without lag: " + listed)
                        .filteredOn(group -> group.groupId().equals(share))
                        .extracting(ConsumerGroupSummary::type, ConsumerGroupSummary::state, ConsumerGroupSummary::lagApplicable,
                                ConsumerGroupSummary::totalLag, ConsumerGroupSummary::uncommittedPartitions)
                        .containsExactly(tuple("SHARE", "Stable", false, 0L, 0));
            });
        }
    }

    @Test
    void listsTheLagOfConsumerGroupsWhenTheClusterAlsoHasAShareGroup() throws Exception {
        // given: a share group directly on the broker
        String share = freshGroup();
        try (var shareConsumer = joinShareGroupOnBroker(share)) {

            // and a classic group through the gateway, with a commit and its consumer still alive
            String classic = freshGroup();
            try (var consumer = joinThroughGateway(classic, ORDERS)) {
                consumer.commitSync(Map.of(new TopicPartition(ORDERS, 0), new OffsetAndMetadata(COMMITTED)));

                // when / then
                eventually(() -> {
                    List<ConsumerGroupSummary> listed = groups.list();
                    assertThat(listed)
                            .withFailMessage(() -> "The listing did not show classic group '" + classic + "' with lag 6 next to share group '"
                                                   + share + "': " + listed)
                            .filteredOn(group -> group.groupId().equals(classic))
                            .extracting(ConsumerGroupSummary::type, ConsumerGroupSummary::lagApplicable, ConsumerGroupSummary::totalLag)
                            .containsExactly(tuple("CLASSIC", true, LAG));
                    assertThat(listed)
                            .withFailMessage(() -> "The listing did not show share group '" + share + "': " + listed)
                            .extracting(ConsumerGroupSummary::groupId)
                            .contains(share);
                });
            }
        }
    }

    @Test
    void describesAShareGroupWithItsStateAndLagNotApplicable() throws Exception {
        // given
        String share = freshGroup();

        try (var consumer = joinShareGroupOnBroker(share)) {
            // when / then
            eventually(() -> {
                ConsumerGroupDetail detail = groups.describe(share);
                assertThat(detail)
                        .withFailMessage(() -> "Share group '" + share + "' was not described as a Stable SHARE group without lag: " + detail)
                        .isEqualTo(new ConsumerGroupDetail(share, "SHARE", "Stable", false, List.of(), List.of(), 0, 0));
            });
        }
    }

    @Test
    void describesAKip848ConsumerGroupWithItsLag() throws Exception {
        // given
        String group = freshGroup();

        try (var consumer = joinKip848OnBroker(group)) {
            consumer.commitSync(Map.of(new TopicPartition(ORDERS_V2, 0), new OffsetAndMetadata(COMMITTED)));

            // when / then
            eventually(() -> {
                ConsumerGroupDetail detail = groups.describe(group);
                assertThat(detail.type())
                        .withFailMessage(() -> "KIP-848 group '" + group + "' was not described as CONSUMER: " + detail)
                        .isEqualTo("CONSUMER");
                assertThat(detail.lagApplicable())
                        .withFailMessage(() -> "KIP-848 group '" + group + "' was not described with lag applicable: " + detail)
                        .isTrue();
                assertThat(partition0(detail.topics().getFirst()))
                        .withFailMessage(() -> "Partition 0 did not show committed 4, end 10 and lag 6: " + detail)
                        .isEqualTo(new PartitionLag(0, COMMITTED, (long) RECORDS, LAG));
                assertThat(detail.totalLag())
                        .withFailMessage(() -> "Total lag of KIP-848 group '" + group + "' was not 6: " + detail)
                        .isEqualTo(LAG);
            });
        }
    }

    /// The gateway's virtual topic configuration, as the service needs it: the same mapping the gateway starts with.
    private VirtualTopicManager virtualTopicManager() {
        Map<String, VirtualTopicConfig> configs = new HashMap<>();
        virtualTopics().forEach((virtual, physical) -> configs.put(virtual, new VirtualTopicConfig(physical)));
        configs.putAll(filteredVirtualTopics());
        return new VirtualTopicManager(configs);
    }

    /// A consumer in `group` through the gateway, subscribed to `topic`, assigned a partition and having read
    /// every fixture record. Polling is what drives the join, so the records are read here, once; the test then
    /// commits explicitly.
    private KafkaConsumer<String, String> joinThroughGateway(String group, String topic) {
        Properties props = saslProps(gatewayBootstrap);
        props.put(GROUP_ID_CONFIG, group);
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        var consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(List.of(topic));
        List<ConsumerRecord<String, String>> read = new ArrayList<>();
        eventually(() -> {
            consumer.poll(Duration.ofMillis(200)).forEach(read::add);
            assertThat(consumer.assignment())
                    .withFailMessage(() -> "Consumer of group '" + group + "' was not assigned a partition of '" + topic + "'")
                    .isNotEmpty();
            assertThat(read)
                    .withFailMessage(() -> "Consumer of group '" + group + "' read " + read.size() + " of " + RECORDS
                                           + " records from '" + topic + "'")
                    .hasSize(RECORDS);
        });
        return consumer;
    }

    /// A consumer that uses KIP-848's `consumer` group protocol, directly on the broker, subscribed to
    /// `orders-v2` and assigned a partition.
    private KafkaConsumer<String, String> joinKip848OnBroker(String group) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap);
        props.put(GROUP_ID_CONFIG, group);
        props.put(GROUP_PROTOCOL_CONFIG, "consumer");
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        var consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(List.of(ORDERS_V2));
        eventually(() -> {
            consumer.poll(Duration.ofMillis(200));
            assertThat(consumer.assignment())
                    .withFailMessage(() -> "KIP-848 consumer of group '" + group + "' was not assigned a partition of '" + ORDERS_V2 + "'")
                    .isNotEmpty();
        });
        return consumer;
    }

    /// A share consumer (KIP-932) of `group`, directly on the broker, subscribed to `orders-v2` and having read
    /// every fixture record, which it then acknowledges. A share group starts at the latest offset by default, so
    /// the group is set to start at the earliest before the consumer joins.
    private KafkaShareConsumer<String, String> joinShareGroupOnBroker(String group) throws Exception {
        GatewayTestSupport.brokerAdmin.incrementalAlterConfigs(Map.of(
                new ConfigResource(ConfigResource.Type.GROUP, group),
                List.of(new AlterConfigOp(new ConfigEntry("share.auto.offset.reset", "earliest"), AlterConfigOp.OpType.SET))))
                .all().get();
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap);
        props.put(GROUP_ID_CONFIG, group);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        var consumer = new KafkaShareConsumer<String, String>(props);
        consumer.subscribe(List.of(ORDERS_V2));
        List<ConsumerRecord<String, String>> read = new ArrayList<>();
        eventually(() -> {
            consumer.poll(Duration.ofMillis(200)).forEach(read::add);
            assertThat(read)
                    .withFailMessage(() -> "Share consumer of group '" + group + "' read " + read.size() + " of " + RECORDS
                                           + " records from '" + ORDERS_V2 + "'")
                    .hasSize(RECORDS);
        });
        consumer.commitSync();
        return consumer;
    }

    private static PartitionLag partition0(TopicLag topic) {
        return topic.partitions().stream()
                .filter(partition -> partition.partition() == 0)
                .findFirst()
                .orElseThrow(() -> new AssertionError("partition 0 is not listed on '" + topic.physicalTopic() + "'"));
    }

    private static String freshGroup() {
        return "consumer-groups-it-" + UUID.randomUUID();
    }

    private static List<Header> tenantAcme() {
        return List.of(new RecordHeader("tenant", "acme".getBytes(StandardCharsets.UTF_8)));
    }
}
