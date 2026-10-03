package io.jonasg.kawa.protocol.kafka;

import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Message;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/// Registry of the Kafka APIs the gateway can decode. APIs not registered here are passed
/// through to the broker un-inspected. Adding a new API = adding one entry (plus its
/// interceptor rewriter).
public final class KafkaApiRegistry {

    public static final short API_VERSIONS = 18;
    public static final short PRODUCE = 0;
    public static final short FETCH = 1;
    public static final short LIST_OFFSETS = 2;
    public static final short METADATA = 3;
    public static final short OFFSET_COMMIT = 8;
    public static final short OFFSET_FETCH = 9;
    public static final short FIND_COORDINATOR = 10;
    public static final short CREATE_TOPICS = 19;
    public static final short CREATE_PARTITIONS = 37;
    public static final short DELETE_TOPICS = 20;
    public static final short TXN_OFFSET_COMMIT = 28;
    public static final short DESCRIBE_CONFIGS = 32;
    public static final short CREATE_ACLS = 30;
    public static final short DELETE_ACLS = 31;
    public static final short DESCRIBE_ACLS = 29;
    public static final short DESCRIBE_TRANSACTIONS = 65;
    public static final short ADD_PARTITIONS_TO_TXN = 24;
    public static final short OFFSET_DELETE = 47;
    public static final short ALTER_CONFIGS = 33;
    public static final short INCREMENTAL_ALTER_CONFIGS = 44;
    public static final short DELETE_RECORDS = 21;
    public static final short SASL_HANDSHAKE = 17;
    public static final short SASL_AUTHENTICATE = 36;
    public static final short JOIN_GROUP = 11;
    public static final short HEARTBEAT = 12;
    public static final short LEAVE_GROUP = 13;
    public static final short SYNC_GROUP = 14;
    public static final short DESCRIBE_GROUPS = 15;
    public static final short LIST_GROUPS = 16;
    public static final short OFFSET_FOR_LEADER_EPOCH = 23;
    public static final short DESCRIBE_LOG_DIRS = 35;
    public static final short DESCRIBE_TOPIC_PARTITIONS = 75;
    public static final short INIT_PRODUCER_ID = 22;
    public static final short CONSUMER_GROUP_HEARTBEAT = 68;

    private final Map<Short, KafkaApiSpec> specs;

    public KafkaApiRegistry(Collection<KafkaApiSpec> specs) {
        Map<Short, KafkaApiSpec> map = new HashMap<>();
        for (KafkaApiSpec spec : specs) {
            if (map.put(spec.apiKey(), spec) != null) {
                throw new IllegalArgumentException("Duplicate spec for api key " + spec.apiKey());
            }
        }
        this.specs = Collections.unmodifiableMap(map);
    }

    public static KafkaApiRegistry create() {
        return new KafkaApiRegistry(List.of(
                new KafkaApiSpec(API_VERSIONS, "ApiVersions", VersionRange.of(0, 3),
                        of(ApiVersionsRequestData::new), of(ApiVersionsResponseData::new)),
                new KafkaApiSpec(METADATA, "Metadata", VersionRange.of(0, 8),
                        of(MetadataRequestData::new), of(MetadataResponseData::new)),
                new KafkaApiSpec(PRODUCE, "Produce", VersionRange.of(0, 8),
                        of(ProduceRequestData::new), of(ProduceResponseData::new)),
                new KafkaApiSpec(FETCH, "Fetch", VersionRange.of(0, 11),
                        of(FetchRequestData::new), of(FetchResponseData::new)),
                new KafkaApiSpec(LIST_OFFSETS, "ListOffsets", VersionRange.of(0, 5),
                        of(ListOffsetsRequestData::new), of(ListOffsetsResponseData::new)),
                new KafkaApiSpec(OFFSET_COMMIT, "OffsetCommit", VersionRange.of(0, 7),
                        of(OffsetCommitRequestData::new), of(OffsetCommitResponseData::new)),
                new KafkaApiSpec(OFFSET_FETCH, "OffsetFetch", VersionRange.of(0, 5),
                        of(OffsetFetchRequestData::new), of(OffsetFetchResponseData::new)),
                new KafkaApiSpec(FIND_COORDINATOR, "FindCoordinator", VersionRange.of(0, 2),
                        of(FindCoordinatorRequestData::new), of(FindCoordinatorResponseData::new)),
                new KafkaApiSpec(CREATE_TOPICS, "CreateTopics", VersionRange.of(0, 7),
                        of(CreateTopicsRequestData::new), of(CreateTopicsResponseData::new)),
                new KafkaApiSpec(CREATE_PARTITIONS, "CreatePartitions", VersionRange.of(0, 3),
                        of(CreatePartitionsRequestData::new), of(CreatePartitionsResponseData::new)),
                new KafkaApiSpec(DELETE_TOPICS, "DeleteTopics", VersionRange.of(0, 6),
                        of(DeleteTopicsRequestData::new), of(DeleteTopicsResponseData::new)),
                new KafkaApiSpec(TXN_OFFSET_COMMIT, "TxnOffsetCommit", VersionRange.of(0, 4),
                        of(TxnOffsetCommitRequestData::new), of(TxnOffsetCommitResponseData::new)),
                new KafkaApiSpec(DESCRIBE_CONFIGS, "DescribeConfigs", VersionRange.of(0, 4),
                        of(DescribeConfigsRequestData::new), of(DescribeConfigsResponseData::new)),
                new KafkaApiSpec(CREATE_ACLS, "CreateAcls", VersionRange.of(0, 3),
                        of(CreateAclsRequestData::new), of(CreateAclsResponseData::new)),
                new KafkaApiSpec(DELETE_ACLS, "DeleteAcls", VersionRange.of(0, 3),
                        of(DeleteAclsRequestData::new), of(DeleteAclsResponseData::new)),
                new KafkaApiSpec(DESCRIBE_ACLS, "DescribeAcls", VersionRange.of(0, 3),
                        of(DescribeAclsRequestData::new), of(DescribeAclsResponseData::new)),
                new KafkaApiSpec(DESCRIBE_TRANSACTIONS, "DescribeTransactions", VersionRange.of(0, 0),
                        of(DescribeTransactionsRequestData::new), of(DescribeTransactionsResponseData::new)),
                new KafkaApiSpec(ADD_PARTITIONS_TO_TXN, "AddPartitionsToTxn", VersionRange.of(0, 3),
                        of(AddPartitionsToTxnRequestData::new), of(AddPartitionsToTxnResponseData::new)),
                new KafkaApiSpec(OFFSET_DELETE, "OffsetDelete", VersionRange.of(0, 0),
                        of(OffsetDeleteRequestData::new), of(OffsetDeleteResponseData::new)),
                new KafkaApiSpec(ALTER_CONFIGS, "AlterConfigs", VersionRange.of(0, 2),
                        of(AlterConfigsRequestData::new), of(AlterConfigsResponseData::new)),
                new KafkaApiSpec(INCREMENTAL_ALTER_CONFIGS, "IncrementalAlterConfigs", VersionRange.of(0, 1),
                        of(IncrementalAlterConfigsRequestData::new), of(IncrementalAlterConfigsResponseData::new)),
                new KafkaApiSpec(DELETE_RECORDS, "DeleteRecords", VersionRange.of(0, 2),
                        of(DeleteRecordsRequestData::new), of(DeleteRecordsResponseData::new)),
                new KafkaApiSpec(SASL_HANDSHAKE, "SaslHandshake", VersionRange.of(0, 1),
                        of(SaslHandshakeRequestData::new), of(SaslHandshakeResponseData::new)),
                new KafkaApiSpec(SASL_AUTHENTICATE, "SaslAuthenticate", VersionRange.of(0, 2),
                        of(SaslAuthenticateRequestData::new), of(SaslAuthenticateResponseData::new)),
                new KafkaApiSpec(JOIN_GROUP, "JoinGroup", VersionRange.of(0, 9),
                        of(JoinGroupRequestData::new), of(JoinGroupResponseData::new)),
                new KafkaApiSpec(SYNC_GROUP, "SyncGroup", VersionRange.of(0, 5),
                        of(SyncGroupRequestData::new), of(SyncGroupResponseData::new)),
                new KafkaApiSpec(DESCRIBE_GROUPS, "DescribeGroups", VersionRange.of(0, 6),
                        of(DescribeGroupsRequestData::new), of(DescribeGroupsResponseData::new)),
                new KafkaApiSpec(LIST_GROUPS, "ListGroups", VersionRange.of(0, 5),
                        of(ListGroupsRequestData::new), of(ListGroupsResponseData::new)),
                new KafkaApiSpec(OFFSET_FOR_LEADER_EPOCH, "OffsetForLeaderEpoch", VersionRange.of(2, 4),
                        of(OffsetForLeaderEpochRequestData::new), of(OffsetForLeaderEpochResponseData::new)),
                new KafkaApiSpec(DESCRIBE_LOG_DIRS, "DescribeLogDirs", VersionRange.of(1, 5),
                        of(DescribeLogDirsRequestData::new), of(DescribeLogDirsResponseData::new)),
                new KafkaApiSpec(DESCRIBE_TOPIC_PARTITIONS, "DescribeTopicPartitions", VersionRange.of(0, 0),
                        of(DescribeTopicPartitionsRequestData::new), of(DescribeTopicPartitionsResponseData::new)),
                new KafkaApiSpec(HEARTBEAT, "Heartbeat", VersionRange.of(0, 4),
                        of(HeartbeatRequestData::new), of(HeartbeatResponseData::new)),
                new KafkaApiSpec(LEAVE_GROUP, "LeaveGroup", VersionRange.of(0, 5),
                        of(LeaveGroupRequestData::new), of(LeaveGroupResponseData::new)),
                new KafkaApiSpec(INIT_PRODUCER_ID, "InitProducerId", bundled(ApiKeys.INIT_PRODUCER_ID),
                        of(InitProducerIdRequestData::new), of(InitProducerIdResponseData::new)),
                new KafkaApiSpec(CONSUMER_GROUP_HEARTBEAT, "ConsumerGroupHeartbeat", bundled(ApiKeys.CONSUMER_GROUP_HEARTBEAT),
                        of(ConsumerGroupHeartbeatRequestData::new), of(ConsumerGroupHeartbeatResponseData::new))));
    }

    /// Every stable version the bundled Kafka client library can decode.
    private static VersionRange bundled(ApiKeys api) {
        return VersionRange.of(api.oldestVersion(), api.latestVersion());
    }

    private static <T extends Message> MessageReader of(Supplier<T> factory) {
        return (readable, version) -> {
            T data = factory.get();
            data.read(readable, version);
            return data;
        };
    }

    /// Spec for the api key, or `null` if the API is not decoded (passthrough).
    public KafkaApiSpec spec(short apiKey) {
        return specs.get(apiKey);
    }

    public Collection<KafkaApiSpec> specs() {
        return specs.values();
    }
}
