package io.jonasg.kawa.test;

import io.jonasg.kawa.broker.BrokerAdmin;
import io.jonasg.kawa.governance.TopicDescriber.TopicState;
import io.jonasg.kawa.governance.TopicSpec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// In-memory [BrokerAdmin] for handler tests: records create/delete calls and describes from
/// [#states] instead of talking to a broker. Consumer groups, their commits and end offsets are
/// set up through [#groups], [#committed] and [#latest].
public final class FakeBrokerAdmin implements BrokerAdmin {

    /// The group types that can be listed with a protocol type of `consumer` by default.
    private static final List<String> CONSUMER_TYPES = List.of("CLASSIC", "CONSUMER");

    public final List<TopicSpec> created = new ArrayList<>();
    public final List<String> deleted = new ArrayList<>();
    public RuntimeException createError;
    /// What [#describe] reports, by topic name.
    public final Map<String, TopicState> states = new HashMap<>();
    /// The consumer groups on the broker, by group id; what the group listing and describe report.
    public final Map<String, GroupDescription> groups = new LinkedHashMap<>();
    /// The protocol type each group reports, by group id. A group not set here reports `consumer` when it is
    /// a `CLASSIC` or `CONSUMER` group, and none otherwise.
    public final Map<String, String> protocolTypes = new HashMap<>();
    /// The offsets each group has committed, by group id and partition.
    public final Map<String, Map<PartitionId, Long>> committed = new HashMap<>();
    /// The latest offset of each partition.
    public final Map<PartitionId, Long> latest = new HashMap<>();
    /// When set, every consumer group read fails with it, as a broker that cannot be reached would.
    public RuntimeException consumerGroupError;

    @Override
    public void createTopic(TopicSpec spec) {
        if (createError != null) {
            throw createError;
        }
        created.add(spec);
    }

    @Override
    public void deleteTopic(String name) {
        deleted.add(name);
    }

    @Override
    public CompletionStage<Map<String, TopicState>> describe(Collection<String> topics) {
        Map<String, TopicState> found = new HashMap<>();
        topics.forEach(name -> {
            if (states.containsKey(name)) {
                found.put(name, states.get(name));
            }
        });
        return CompletableFuture.completedFuture(found);
    }

    @Override
    public CompletionStage<List<GroupSummary>> listGroups() {
        if (consumerGroupError != null) {
            return CompletableFuture.failedFuture(consumerGroupError);
        }
        return CompletableFuture.completedFuture(groups.values().stream()
                .map(group -> new GroupSummary(group.groupId(), group.type(), group.state(), protocolTypeOf(group)))
                .toList());
    }

    @Override
    public CompletionStage<Map<String, GroupDescription>> describeConsumerGroups(Collection<String> groupIds) {
        if (consumerGroupError != null) {
            return CompletableFuture.failedFuture(consumerGroupError);
        }
        if (anyNotCommittingOffsets(groupIds)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("the broker does not describe a group that does not commit offsets"));
        }
        Map<String, GroupDescription> found = new HashMap<>();
        groupIds.forEach(groupId -> {
            if (groups.containsKey(groupId)) {
                found.put(groupId, groups.get(groupId));
            }
        });
        return CompletableFuture.completedFuture(found);
    }

    @Override
    public CompletionStage<Map<String, Map<PartitionId, Long>>> committedOffsets(Collection<String> groupIds) {
        if (consumerGroupError != null) {
            return CompletableFuture.failedFuture(consumerGroupError);
        }
        if (anyNotCommittingOffsets(groupIds)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("the broker does not list offsets of a group that does not commit offsets"));
        }
        Map<String, Map<PartitionId, Long>> found = new HashMap<>();
        groupIds.forEach(groupId -> {
            if (groups.containsKey(groupId)) {
                found.put(groupId, committed.getOrDefault(groupId, Map.of()));
            }
        });
        return CompletableFuture.completedFuture(found);
    }

    @Override
    public CompletionStage<Map<PartitionId, Long>> latestOffsets(Collection<PartitionId> partitions) {
        if (consumerGroupError != null) {
            return CompletableFuture.failedFuture(consumerGroupError);
        }
        Map<PartitionId, Long> found = new HashMap<>();
        partitions.forEach(partition -> {
            if (latest.containsKey(partition)) {
                found.put(partition, latest.get(partition));
            }
        });
        return CompletableFuture.completedFuture(found);
    }

    @Override
    public void close() {
    }

    private String protocolTypeOf(GroupDescription group) {
        if (protocolTypes.containsKey(group.groupId())) {
            return protocolTypes.get(group.groupId());
        }
        return CONSUMER_TYPES.contains(group.type()) ? "consumer" : null;
    }

    /// Whether the group commits offsets per partition: a `CONSUMER` group, or a `CLASSIC` group of protocol `consumer`
    /// or of none (a group that only commits offsets, without joining).
    private boolean commitsOffsets(GroupDescription group) {
        String protocolType = protocolTypeOf(group);
        return group.type().equals("CONSUMER") || group.type().equals("CLASSIC") && (protocolType == null || "consumer".equals(protocolType));
    }

    private boolean anyNotCommittingOffsets(Collection<String> groupIds) {
        return groupIds.stream()
                .filter(groups::containsKey)
                .map(groups::get)
                .anyMatch(group -> !commitsOffsets(group));
    }
}
