package io.jonasg.kawa.broker;

import io.jonasg.kawa.governance.TopicDescriber;
import io.jonasg.kawa.governance.TopicSpec;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/// Broker topic and consumer group operations for the admin API. Implementations talk to the real
/// cluster; [KafkaBrokerAdmin] uses Kafka's AdminClient. Describing ([TopicDescriber]) reads a
/// topic's current state for governance. The consumer group reads are read-only.
public interface BrokerAdmin extends TopicDescriber, AutoCloseable {

    /// Creates the topic on the broker. Throws when the broker rejects the request
    /// (e.g. `TopicExistsException` when the topic already exists).
    void createTopic(TopicSpec spec) throws Exception;

    /// Deletes the topic on the broker.
    void deleteTopic(String name) throws Exception;

    /// Every group on the cluster, of every type (consumer, share, streams, and classic groups of any protocol
    /// such as Kafka Connect's), with its type, state and protocol type.
    CompletionStage<List<GroupSummary>> listGroups();

    /// The consumer groups that exist, by id; a group the broker does not know is left out. Every id must name a
    /// group that commits offsets per partition (a `CONSUMER` group, or a `CLASSIC` group of protocol `consumer`):
    /// the broker fails the whole call for any other group.
    CompletionStage<Map<String, GroupDescription>> describeConsumerGroups(Collection<String> groupIds);

    /// The offsets each group has committed, by partition, for all its partitions. A group with no
    /// commits maps to an empty map; a group the broker does not know is left out. As with
    /// [#describeConsumerGroups], every id must name a group that commits offsets per partition.
    CompletionStage<Map<String, Map<PartitionId, Long>>> committedOffsets(Collection<String> groupIds);

    /// The latest offset of each partition as read-committed consumers see it; a partition the broker
    /// does not know is left out.
    CompletionStage<Map<PartitionId, Long>> latestOffsets(Collection<PartitionId> partitions);

    @Override
    void close();

    /// A partition of a physical topic.
    ///
    /// @param topic     the physical topic name
    /// @param partition the partition number
    record PartitionId(String topic, int partition) {
    }

    /// A group in the listing. `type` and `state` are the names the broker reports.
    ///
    /// @param groupId      the group id
    /// @param type         the group type, such as `CLASSIC`, `CONSUMER`, `SHARE` or `STREAMS`
    /// @param state        the group state, such as `Stable` or `Empty`
    /// @param protocolType the protocol type the group reports, such as `consumer` or `connect`; `null` when it reports none
    record GroupSummary(String groupId, String type, String state, @Nullable String protocolType) {
    }

    /// A consumer group with its members.
    ///
    /// @param groupId the group id
    /// @param type    the group type
    /// @param state   the group state
    /// @param members the members and the partitions each is assigned
    record GroupDescription(String groupId, String type, String state, List<GroupMember> members) {
    }

    /// A member of a consumer group.
    ///
    /// @param memberId the member id
    /// @param clientId the client id the member runs with
    /// @param host     the host the member connects from
    /// @param assigned the partitions the member is assigned
    record GroupMember(String memberId, String clientId, String host, List<PartitionId> assigned) {
    }
}
