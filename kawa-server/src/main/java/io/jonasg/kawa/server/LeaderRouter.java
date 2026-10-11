package io.jonasg.kawa.server;

import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.core.Route;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.utils.Utils;

import java.util.List;

/// Routes each request to the broker that must handle it:
///
/// - Partition-scoped requests (Produce, Fetch, ListOffsets, OffsetForLeaderEpoch) go to the
///   leader of their first partition. Requests that span multiple brokers are sent to the first
///   leader; the broker replies with NOT_LEADER_OR_FENCED for the others and the client retries,
///   converging on the correct brokers.
/// - Consumer-group requests go to the group's coordinator, and transactional requests to the
///   transaction's coordinator: the leader of the `__consumer_offsets` or `__transaction_state`
///   partition the group or transactional id maps to, exactly as the brokers map it. The client
///   sent them to its coordinator, but behind the gateway's single advertised endpoint that choice
///   is lost, so the gateway has to find the coordinator again.
/// - Everything else (Metadata, FindCoordinator, ListGroups, ...) goes to any known broker.
///
/// Any request whose broker cannot be resolved (unknown topic, empty id, internal topic not known
/// yet) falls back to any broker, which answers with a retriable error.
public final class LeaderRouter {

    private static final String CONSUMER_OFFSETS_TOPIC = "__consumer_offsets";
    private static final String TRANSACTION_STATE_TOPIC = "__transaction_state";

    private final MetadataCache cache;

    public LeaderRouter(MetadataCache cache) {
        this.cache = cache;
    }

    public Route route(Request request) {
        Object body = request.body();
        if (body == null) {
            return anyBroker();
        }
        int broker = switch ((short) request.apiKey()) {
            case KafkaApiRegistry.PRODUCE -> leaderOfFirstProduceTopic((ProduceRequestData) body);
            case KafkaApiRegistry.FETCH -> leaderOfFirstFetchTopic((FetchRequestData) body);
            case KafkaApiRegistry.LIST_OFFSETS -> leaderOfFirstListOffsetsTopic((ListOffsetsRequestData) body);
            case KafkaApiRegistry.OFFSET_FOR_LEADER_EPOCH ->
                    leaderOfFirstOffsetForLeaderEpochTopic((OffsetForLeaderEpochRequestData) body);
            case KafkaApiRegistry.OFFSET_COMMIT -> leaderOfFirstOffsetCommitTopic((OffsetCommitRequestData) body);
            case KafkaApiRegistry.OFFSET_FETCH -> leaderOfFirstOffsetFetchTopic((OffsetFetchRequestData) body);
            case KafkaApiRegistry.JOIN_GROUP -> groupCoordinator(((JoinGroupRequestData) body).groupId());
            case KafkaApiRegistry.SYNC_GROUP -> groupCoordinator(((SyncGroupRequestData) body).groupId());
            case KafkaApiRegistry.HEARTBEAT -> groupCoordinator(((HeartbeatRequestData) body).groupId());
            case KafkaApiRegistry.LEAVE_GROUP -> groupCoordinator(((LeaveGroupRequestData) body).groupId());
            case KafkaApiRegistry.CONSUMER_GROUP_HEARTBEAT ->
                    groupCoordinator(((ConsumerGroupHeartbeatRequestData) body).groupId());
            case KafkaApiRegistry.OFFSET_DELETE -> groupCoordinator(((OffsetDeleteRequestData) body).groupId());
            case KafkaApiRegistry.DESCRIBE_GROUPS -> groupCoordinator(first(((DescribeGroupsRequestData) body).groups()));
            // TxnOffsetCommit writes the group's offsets, so it goes to the group coordinator
            case KafkaApiRegistry.TXN_OFFSET_COMMIT -> groupCoordinator(((TxnOffsetCommitRequestData) body).groupId());
            // a non-transactional InitProducerId (an idempotent producer) has no id and goes to any broker
            case KafkaApiRegistry.INIT_PRODUCER_ID ->
                    transactionCoordinator(((InitProducerIdRequestData) body).transactionalId());
            // only v0-3 is registered: the v4+ batch form is sent by brokers, not clients
            case KafkaApiRegistry.ADD_PARTITIONS_TO_TXN ->
                    transactionCoordinator(((AddPartitionsToTxnRequestData) body).v3AndBelowTransactionalId());
            case KafkaApiRegistry.ADD_OFFSETS_TO_TXN ->
                    transactionCoordinator(((AddOffsetsToTxnRequestData) body).transactionalId());
            case KafkaApiRegistry.END_TXN -> transactionCoordinator(((EndTxnRequestData) body).transactionalId());
            case KafkaApiRegistry.DESCRIBE_TRANSACTIONS ->
                    transactionCoordinator(first(((DescribeTransactionsRequestData) body).transactionalIds()));
            default -> -1;
        };
        return broker >= 0 ? Route.to(broker) : anyBroker();
    }

    private int leaderOfFirstProduceTopic(ProduceRequestData data) {
        for (ProduceRequestData.TopicProduceData topic : data.topicData()) {
            for (ProduceRequestData.PartitionProduceData partition : topic.partitionData()) {
                int leader = cache.leaderFor(topic.name(), partition.index());
                if (leader >= 0) {
                    return leader;
                }
            }
        }
        return -1;
    }

    private int leaderOfFirstFetchTopic(FetchRequestData data) {
        for (FetchRequestData.FetchTopic topic : data.topics()) {
            for (FetchRequestData.FetchPartition partition : topic.partitions()) {
                int leader = cache.leaderFor(topic.topic(), partition.partition());
                if (leader >= 0) {
                    return leader;
                }
            }
        }
        return -1;
    }

    private int leaderOfFirstListOffsetsTopic(ListOffsetsRequestData data) {
        for (ListOffsetsRequestData.ListOffsetsTopic topic : data.topics()) {
            for (ListOffsetsRequestData.ListOffsetsPartition partition : topic.partitions()) {
                int leader = cache.leaderFor(topic.name(), partition.partitionIndex());
                if (leader >= 0) {
                    return leader;
                }
            }
        }
        return -1;
    }

    private int leaderOfFirstOffsetForLeaderEpochTopic(OffsetForLeaderEpochRequestData data) {
        for (OffsetForLeaderEpochRequestData.OffsetForLeaderTopic topic : data.topics()) {
            for (OffsetForLeaderEpochRequestData.OffsetForLeaderPartition partition : topic.partitions()) {
                int leader = cache.leaderFor(topic.topic(), partition.partition());
                if (leader >= 0) {
                    return leader;
                }
            }
        }
        return -1;
    }

    private int leaderOfFirstOffsetCommitTopic(OffsetCommitRequestData data) {
        int coordinator = groupCoordinator(data.groupId());
        if (coordinator >= 0) {
            return coordinator;
        }
        for (OffsetCommitRequestData.OffsetCommitRequestTopic topic : data.topics()) {
            int leader = cache.leaderForAny(topic.name());
            if (leader >= 0) {
                return leader;
            }
        }
        return -1;
    }

    private int leaderOfFirstOffsetFetchTopic(OffsetFetchRequestData data) {
        int coordinator = groupCoordinator(data.groupId());
        if (coordinator >= 0) {
            return coordinator;
        }
        for (OffsetFetchRequestData.OffsetFetchRequestTopic topic : data.topics()) {
            int leader = cache.leaderForAny(topic.name());
            if (leader >= 0) {
                return leader;
            }
        }
        return -1;
    }

    private int groupCoordinator(String groupId) {
        return coordinatorFor(groupId, CONSUMER_OFFSETS_TOPIC);
    }

    private int transactionCoordinator(String transactionalId) {
        return coordinatorFor(transactionalId, TRANSACTION_STATE_TOPIC);
    }

    /// The leader of the `internalTopic` partition that `key` maps to, the way the brokers map it
    /// (`GroupCoordinatorService.partitionFor` / `TransactionStateManager.partitionFor`), or `-1`
    /// when the key is empty or the partition or its leader is not known yet.
    private int coordinatorFor(String key, String internalTopic) {
        if (key == null || key.isEmpty()) {
            return -1;
        }
        int partitionCount = cache.partitionCount(internalTopic);
        if (partitionCount <= 0) {
            return -1;
        }
        return cache.leaderFor(internalTopic, Utils.abs(key.hashCode()) % partitionCount);
    }

    private static String first(List<String> ids) {
        return ids.isEmpty() ? null : ids.getFirst();
    }

    private int anyBrokerId() {
        return cache.brokers().stream()
                .mapToInt(BrokerNode::id)
                .min()
                .orElse(-1);
    }

    private Route anyBroker() {
        int brokerId = anyBrokerId();
        return brokerId >= 0 ? Route.to(brokerId) : Route.UNROUTED;
    }
}
