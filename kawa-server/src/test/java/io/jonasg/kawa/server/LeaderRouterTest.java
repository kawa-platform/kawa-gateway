package io.jonasg.kawa.server;

import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.core.Route;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataSnapshot;
import io.jonasg.kawa.core.cluster.PartitionMetadata;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class LeaderRouterTest {

    /// Coordinated by broker 3: `__consumer_offsets` partition `abs("orders-readers".hashCode()) % 6`
    /// = 2. Neither the lowest broker (1) nor murmur2 (which maps it to broker 2) would get there.
    private static final String GROUP = "orders-readers";
    private static final int GROUP_COORDINATOR = 3;
    /// Coordinated by broker 3: `__transaction_state` partition 2 (murmur2 would pick broker 1).
    private static final String TRANSACTIONAL_ID = "txn-billing";
    private static final int TRANSACTION_COORDINATOR = 3;
    private static final int INTERNAL_PARTITIONS = 6;

    /// Brokers 1-3. Both internal topics have 6 partitions; partition `p` is led by broker
    /// `p % 3 + 1`, so the coordinators are spread over the brokers.
    private static MetadataCache cacheWithTopology() {
        var cache = new MetadataCache();
        cache.update(MetadataSnapshot.of(
                Map.of(
                        "orders-v2", TopicMetadata.of("orders-v2", List.of(
                                PartitionMetadata.of(0, 1, List.of(1), List.of(1), List.of()),
                                PartitionMetadata.of(1, 2, List.of(2), List.of(2), List.of()))),
                        "customers-v2", TopicMetadata.of("customers-v2", List.of(
                                PartitionMetadata.of(0, 2, List.of(2), List.of(2), List.of()))),
                        "__consumer_offsets", internalTopic("__consumer_offsets"),
                        "__transaction_state", internalTopic("__transaction_state")),
                Map.of(
                        1, BrokerNode.of(1, "broker-1", 9092, null),
                        2, BrokerNode.of(2, "broker-2", 9093, null),
                        3, BrokerNode.of(3, "broker-3", 9094, null)),
                "cluster-1"));
        return cache;
    }

    private static TopicMetadata internalTopic(String name) {
        return TopicMetadata.of(name, IntStream.range(0, INTERNAL_PARTITIONS)
                .mapToObj(p -> PartitionMetadata.of(p, p % 3 + 1, List.of(p % 3 + 1), List.of(p % 3 + 1), List.of()))
                .toList());
    }

    @Test
    void mapsIdsToTheirCoordinatorTheWayKafkaDoes() {
        // the fixtures above are computed with Kafka's own mapping, Utils.abs(id.hashCode()) % partitions
        assertThat(Utils.abs(GROUP.hashCode()) % INTERNAL_PARTITIONS % 3 + 1).isEqualTo(GROUP_COORDINATOR);
        assertThat(Utils.abs(TRANSACTIONAL_ID.hashCode()) % INTERNAL_PARTITIONS % 3 + 1).isEqualTo(TRANSACTION_COORDINATOR);
    }

    @Test
    void routesProduceToPartitionLeader() {
        var router = new LeaderRouter(cacheWithTopology());

        var data = new ProduceRequestData();
        data.topicData().add(new ProduceRequestData.TopicProduceData().setName("orders-v2")
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData().setIndex(1))));

        Route route = router.route(request(0, data));
        assertThat(route.brokerId()).isEqualTo(2);
    }

    @Test
    void routesOffsetForLeaderEpochToPartitionLeader() {
        // given
        var router = new LeaderRouter(cacheWithTopology());
        var data = new OffsetForLeaderEpochRequestData();
        var topic = new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic().setTopic("orders-v2");
        topic.partitions().add(new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition().setPartition(1));
        data.topics().add(topic);

        // when
        Route route = router.route(request(KafkaApiRegistry.OFFSET_FOR_LEADER_EPOCH, data));

        // then
        assertThat(route.brokerId())
                .withFailMessage(() -> "OffsetForLeaderEpoch for orders-v2/1 went to broker " + route.brokerId()
                        + " instead of its leader, broker 2")
                .isEqualTo(2);
    }

    @Test
    void routesToAnyBrokerForPassthroughRequests() {
        var router = new LeaderRouter(cacheWithTopology());

        Route route = router.route(request(50, null));
        assertThat(route.brokerId()).isEqualTo(1);
    }

    @Test
    void returnsUnroutedWhenTopologyUnknown() {
        var router = new LeaderRouter(new MetadataCache());

        Route route = router.route(request(50, null));
        assertThat(route).isEqualTo(Route.UNROUTED);
    }

    @Test
    void offsetApisShouldRouteByGroupNotByTopicLeader() {
        var router = new LeaderRouter(cacheWithTopology());

        var fetchOnOrders = new OffsetFetchRequestData().setGroupId(GROUP);
        fetchOnOrders.topics().add(new OffsetFetchRequestData.OffsetFetchRequestTopic().setName("orders-v2"));
        var fetchOnCustomers = new OffsetFetchRequestData().setGroupId(GROUP);
        fetchOnCustomers.topics().add(new OffsetFetchRequestData.OffsetFetchRequestTopic().setName("customers-v2"));
        var commitOnOrders = new OffsetCommitRequestData().setGroupId(GROUP);
        commitOnOrders.topics().add(new OffsetCommitRequestData.OffsetCommitRequestTopic().setName("orders-v2"));
        var commitOnCustomers = new OffsetCommitRequestData().setGroupId(GROUP);
        commitOnCustomers.topics().add(new OffsetCommitRequestData.OffsetCommitRequestTopic().setName("customers-v2"));

        assertThat(List.of(
                router.route(request(KafkaApiRegistry.OFFSET_FETCH, fetchOnOrders)).brokerId(),
                router.route(request(KafkaApiRegistry.OFFSET_FETCH, fetchOnCustomers)).brokerId(),
                router.route(request(KafkaApiRegistry.OFFSET_COMMIT, commitOnOrders)).brokerId(),
                router.route(request(KafkaApiRegistry.OFFSET_COMMIT, commitOnCustomers)).brokerId()))
                .describedAs("same group should route to its one coordinator regardless of data topic")
                .containsOnly(GROUP_COORDINATOR);
    }

    static Stream<Arguments> groupScopedRequests() {
        var describeGroups = new DescribeGroupsRequestData();
        describeGroups.groups().add(GROUP);
        return Stream.of(
                Arguments.of("JoinGroup", KafkaApiRegistry.JOIN_GROUP, new JoinGroupRequestData().setGroupId(GROUP)),
                Arguments.of("SyncGroup", KafkaApiRegistry.SYNC_GROUP, new SyncGroupRequestData().setGroupId(GROUP)),
                Arguments.of("Heartbeat", KafkaApiRegistry.HEARTBEAT, new HeartbeatRequestData().setGroupId(GROUP)),
                Arguments.of("LeaveGroup", KafkaApiRegistry.LEAVE_GROUP, new LeaveGroupRequestData().setGroupId(GROUP)),
                Arguments.of("ConsumerGroupHeartbeat", KafkaApiRegistry.CONSUMER_GROUP_HEARTBEAT,
                        new ConsumerGroupHeartbeatRequestData().setGroupId(GROUP)),
                Arguments.of("DescribeGroups", KafkaApiRegistry.DESCRIBE_GROUPS, describeGroups),
                Arguments.of("OffsetDelete", KafkaApiRegistry.OFFSET_DELETE, new OffsetDeleteRequestData().setGroupId(GROUP)),
                Arguments.of("TxnOffsetCommit", KafkaApiRegistry.TXN_OFFSET_COMMIT,
                        new TxnOffsetCommitRequestData().setTransactionalId(TRANSACTIONAL_ID).setGroupId(GROUP)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("groupScopedRequests")
    void routesGroupScopedRequestsToTheGroupCoordinator(String api, short apiKey, Object body) {
        // given
        var router = new LeaderRouter(cacheWithTopology());

        // when
        Route route = router.route(request(apiKey, body));

        // then
        assertThat(route.brokerId())
                .withFailMessage(() -> api + " for group '" + GROUP + "' went to broker " + route.brokerId()
                        + " instead of its coordinator, broker " + GROUP_COORDINATOR)
                .isEqualTo(GROUP_COORDINATOR);
    }

    static Stream<Arguments> transactionScopedRequests() {
        var describeTransactions = new DescribeTransactionsRequestData();
        describeTransactions.transactionalIds().add(TRANSACTIONAL_ID);
        return Stream.of(
                Arguments.of("InitProducerId", KafkaApiRegistry.INIT_PRODUCER_ID,
                        new InitProducerIdRequestData().setTransactionalId(TRANSACTIONAL_ID)),
                Arguments.of("AddPartitionsToTxn", KafkaApiRegistry.ADD_PARTITIONS_TO_TXN,
                        new AddPartitionsToTxnRequestData().setV3AndBelowTransactionalId(TRANSACTIONAL_ID)),
                Arguments.of("AddOffsetsToTxn", KafkaApiRegistry.ADD_OFFSETS_TO_TXN,
                        new AddOffsetsToTxnRequestData().setTransactionalId(TRANSACTIONAL_ID).setGroupId(GROUP)),
                Arguments.of("EndTxn", KafkaApiRegistry.END_TXN, new EndTxnRequestData().setTransactionalId(TRANSACTIONAL_ID)),
                Arguments.of("DescribeTransactions", KafkaApiRegistry.DESCRIBE_TRANSACTIONS, describeTransactions));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("transactionScopedRequests")
    void routesTransactionScopedRequestsToTheTransactionCoordinator(String api, short apiKey, Object body) {
        // given
        var router = new LeaderRouter(cacheWithTopology());

        // when
        Route route = router.route(request(apiKey, body));

        // then
        assertThat(route.brokerId())
                .withFailMessage(() -> api + " for transactional id '" + TRANSACTIONAL_ID + "' went to broker "
                        + route.brokerId() + " instead of its coordinator, broker " + TRANSACTION_COORDINATOR)
                .isEqualTo(TRANSACTION_COORDINATOR);
    }

    static Stream<Arguments> requestsForAnyBroker() {
        return Stream.of(
                Arguments.of("FindCoordinator", KafkaApiRegistry.FIND_COORDINATOR, new FindCoordinatorRequestData().setKey(GROUP)),
                Arguments.of("ListGroups", KafkaApiRegistry.LIST_GROUPS, new ListGroupsRequestData()),
                Arguments.of("idempotent InitProducerId", KafkaApiRegistry.INIT_PRODUCER_ID, new InitProducerIdRequestData()),
                Arguments.of("JoinGroup without a group id", KafkaApiRegistry.JOIN_GROUP, new JoinGroupRequestData().setGroupId("")),
                Arguments.of("DescribeGroups without groups", KafkaApiRegistry.DESCRIBE_GROUPS, new DescribeGroupsRequestData()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("requestsForAnyBroker")
    void routesRequestsWithoutACoordinatorToAnyBroker(String api, short apiKey, Object body) {
        // given
        var router = new LeaderRouter(cacheWithTopology());

        // when
        Route route = router.route(request(apiKey, body));

        // then
        assertThat(route.brokerId())
                .withFailMessage(() -> api + " went to broker " + route.brokerId() + " instead of any broker (1)")
                .isEqualTo(1);
    }

    @Test
    void fallsBackToAnyBrokerWhileTheCoordinatorTopicsAreUnknown() {
        // given - brokers known, but neither internal topic yet
        var cache = new MetadataCache();
        cache.update(MetadataSnapshot.of(Map.of(),
                Map.of(1, BrokerNode.of(1, "broker-1", 9092, null), 2, BrokerNode.of(2, "broker-2", 9093, null)),
                "cluster-1"));
        var router = new LeaderRouter(cache);

        // when
        Route group = router.route(request(KafkaApiRegistry.JOIN_GROUP, new JoinGroupRequestData().setGroupId(GROUP)));
        Route transaction = router.route(request(KafkaApiRegistry.END_TXN, new EndTxnRequestData().setTransactionalId(TRANSACTIONAL_ID)));

        // then
        assertThat(List.of(group.brokerId(), transaction.brokerId()))
                .withFailMessage(() -> "Requests whose coordinator is unknown did not fall back to any broker: "
                        + group + ", " + transaction)
                .containsOnly(1);
    }

    private static Request request(
            int apiKey,
            Object body
    ) {
        return new Request() {
            @Override
            public int apiKey() {
                return apiKey;
            }

            @Override
            public String apiName() {
                return "test";
            }

            @Override
            public short apiVersion() {
                return 8;
            }

            @Override
            public int correlationId() {
                return 1;
            }

            @Override
            public String clientId() {
                return "test";
            }

            @Override
            public Object body() {
                return body;
            }
        };
    }
}
