package io.jonasg.kawa.http.topics;

import io.jonasg.kawa.governance.TopicDescriber;
import io.jonasg.kawa.governance.TopicSpec;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/// Broker topic operations for the admin HTTP surface. Implementations talk to the real
/// cluster; [KafkaTopicAdmin] uses Kafka's AdminClient. Describing ([TopicDescriber]) reads a
/// topic's current state for governance. The consumer group reads are read-only: the gateway
/// proxies group coordination to the broker, so the broker knows every client's group.
public interface TopicAdmin extends TopicDescriber, AutoCloseable {

    /// Creates the topic on the broker. Throws when the broker rejects the request
    /// (e.g. `TopicExistsException` when the topic already exists).
    void createTopic(TopicSpec spec) throws Exception;

    /// Deletes the topic on the broker.
    void deleteTopic(String name) throws Exception;

    /// Every consumer group on the broker (classic and consumer protocol), in one call.
    List<GroupSummary> listConsumerGroups() throws Exception;

    /// The groups among `groupIds` the broker knows, by id, in one call. A group the broker does
    /// not know is left out.
    Map<String, GroupDetails> describeConsumerGroups(Collection<String> groupIds) throws Exception;

    /// The committed offset of every partition each of `groupIds` has committed, by group id, in
    /// one call. A partition without a committed offset is left out.
    Map<String, Map<GroupPartition, Long>> committedOffsets(Collection<String> groupIds) throws Exception;

    /// The end offset (high watermark) of each of `partitions`, in one call. A partition the broker
    /// does not know, e.g. because its topic was deleted, is left out.
    Map<GroupPartition, Long> endOffsets(Collection<GroupPartition> partitions) throws Exception;

    @Override
    void close();

    /// A consumer group as listed by the broker.
    ///
    /// @param groupId the group's id
    /// @param state   the group's state as the broker names it, e.g. `Stable`, `Empty` or `PreparingRebalance`
    record GroupSummary(String groupId, String state) {
    }

    /// A consumer group with its members.
    ///
    /// @param groupId the group's id
    /// @param state   the group's state as the broker names it
    /// @param members the group's current members
    record GroupDetails(String groupId, String state, List<GroupMember> members) {
    }

    /// A member of a consumer group.
    ///
    /// @param memberId   the id the coordinator gave the member
    /// @param clientId   the client id the consumer sent
    /// @param host       the host the consumer connected from, as the broker saw it
    /// @param assignment the partitions assigned to the member
    record GroupMember(String memberId, String clientId, String host, List<GroupPartition> assignment) {
    }

    /// A partition of a physical topic.
    record GroupPartition(String topic, int partition) {
    }
}
