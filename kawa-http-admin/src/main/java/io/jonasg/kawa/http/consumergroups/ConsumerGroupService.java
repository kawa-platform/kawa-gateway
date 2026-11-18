package io.jonasg.kawa.http.consumergroups;

import io.jonasg.kawa.broker.BrokerAdmin;
import io.jonasg.kawa.broker.BrokerAdmin.GroupDescription;
import io.jonasg.kawa.broker.BrokerAdmin.GroupMember;
import io.jonasg.kawa.broker.BrokerAdmin.GroupSummary;
import io.jonasg.kawa.broker.BrokerAdmin.PartitionId;
import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/// The groups on the cluster and, where it applies, their lag, read from the broker on every call. Every group
/// the broker reports is listed: consumer, share, streams, and classic groups of any protocol. Lag applies only to
/// groups that commit offsets per partition (see [#lagApplies]); the broker refuses to describe or list the offsets
/// of any other group and fails the whole batched call, so only those groups are sent to it. Offsets belong to physical
/// partitions, so lag is reported per physical topic, together with the virtual topics that point at it
/// and whether any of them filters records: the lag of a filtered virtual topic is an upper bound, since the
/// records its filter drops still count as lag but are never delivered. A group is not attributed to a
/// virtual topic, because the broker does not say which one it read.
///
/// Lag is end offset minus committed offset, summed over the partitions that have a commit. A partition
/// the group reads without a commit is counted separately rather than as zero.
public final class ConsumerGroupService {

    private static final int BROKER_TIMEOUT_SECONDS = 30;
    /// The state the broker reports for a group it no longer holds (Kafka's `GroupState.DEAD`).
    private static final String DEAD = "Dead";

    private final VirtualTopicManager virtualTopics;
    private final BrokerAdmin brokerAdmin;

    public ConsumerGroupService(VirtualTopicManager virtualTopics, BrokerAdmin brokerAdmin) {
        this.virtualTopics = virtualTopics;
        this.brokerAdmin = brokerAdmin;
    }

    /// Every group on the cluster, with its type, state and protocol type and, where lag applies, its total lag and
    /// whether that lag is an upper bound.
    public List<ConsumerGroupSummary> list() {
        List<GroupSummary> groups = await(brokerAdmin.listGroups());
        Map<String, ConsumerGroupDetail> details = details(groups);
        return groups.stream()
                .filter(group -> details.containsKey(group.groupId()))
                .map(group -> summary(group, details.get(group.groupId())))
                .toList();
    }

    /// One group. Where lag applies: its members and, for each partition it reads, the committed offset, end offset
    /// and lag. For any other group only its type and state, with no members or topics.
    ///
    /// @throws NotFoundException when the broker does not know the group
    public ConsumerGroupDetail describe(String groupId) {
        List<GroupSummary> listed = await(brokerAdmin.listGroups()).stream()
                .filter(group -> group.groupId().equals(groupId))
                .toList();
        ConsumerGroupDetail detail = details(listed).get(groupId);
        if (detail == null) {
            throw new NotFoundException("consumer group '" + groupId + "' not found");
        }
        return detail;
    }

    /// Whether the group commits offsets per partition, so that lag applies: a `CONSUMER` group, or a `CLASSIC` group
    /// of protocol type `consumer` or of none (a group that only commits offsets, without joining).
    private static boolean lagApplies(GroupSummary group) {
        return "CONSUMER".equals(group.type())
               || "CLASSIC".equals(group.type()) && (group.protocolType() == null || "consumer".equals(group.protocolType()));
    }

    /// The listed groups that exist, in the order listed. A group lag applies to is read with its lag; any other
    /// group as listed, without lag.
    private Map<String, ConsumerGroupDetail> details(List<GroupSummary> groups) {
        Map<String, ConsumerGroupDetail> withLag = lagDetails(groups.stream()
                .filter(ConsumerGroupService::lagApplies)
                .map(GroupSummary::groupId)
                .toList());
        Map<String, ConsumerGroupDetail> details = new LinkedHashMap<>();
        for (GroupSummary group : groups) {
            if (!lagApplies(group)) {
                details.put(group.groupId(), new ConsumerGroupDetail(group.groupId(), group.type(), group.state(), false,
                        List.of(), List.of(), 0, 0));
            } else if (withLag.containsKey(group.groupId())) {
                details.put(group.groupId(), withLag.get(group.groupId()));
            }
        }
        return details;
    }

    /// The groups that exist, in the order asked, with their lag. Every id must name a group lag applies to. Reads
    /// are batched: one call per broker operation covers every group.
    private Map<String, ConsumerGroupDetail> lagDetails(List<String> groupIds) {
        if (groupIds.isEmpty()) {
            return Map.of();
        }
        Map<String, GroupDescription> described = await(brokerAdmin.describeConsumerGroups(groupIds));
        Map<String, Map<PartitionId, Long>> committed = await(brokerAdmin.committedOffsets(groupIds));

        Map<String, GroupDescription> groups = new LinkedHashMap<>();
        for (String groupId : groupIds) {
            GroupDescription group = described.get(groupId);
            if (group != null && !isUnknown(group, committed.getOrDefault(groupId, Map.of()))) {
                groups.put(groupId, withPhysicalAssignments(group));
            }
        }
        Set<PartitionId> partitions = new HashSet<>();
        groups.values().forEach(group -> group.members().forEach(member -> partitions.addAll(member.assigned())));
        committed.values().forEach(offsets -> partitions.addAll(offsets.keySet()));
        Map<PartitionId, Long> latest = partitions.isEmpty() ? Map.of() : await(brokerAdmin.latestOffsets(partitions));

        Map<String, ConsumerGroupDetail> details = new LinkedHashMap<>();
        groups.forEach((groupId, group) ->
                details.put(groupId, detail(group, committed.getOrDefault(groupId, Map.of()), latest)));
        return details;
    }

    /// The broker reports a group it no longer holds as `Dead`, with no members and no commits: that group is unknown.
    private static boolean isUnknown(GroupDescription group, Map<PartitionId, Long> committed) {
        return DEAD.equals(group.state()) && group.members().isEmpty() && committed.isEmpty();
    }

    /// Members name topics as the client was given them, so an assignment can name a virtual topic. Offsets
    /// belong to the physical topic, so the assignment is mapped to it.
    private GroupDescription withPhysicalAssignments(GroupDescription group) {
        var members = group.members().stream()
                .map(member -> new GroupMember(member.memberId(), member.clientId(), member.host(),
                        member.assigned().stream().map(this::physical).toList()))
                .toList();
        return new GroupDescription(group.groupId(), group.type(), group.state(), members);
    }

    private PartitionId physical(PartitionId partition) {
        return new PartitionId(virtualTopics.toPhysical(partition.topic()), partition.partition());
    }

    private ConsumerGroupDetail detail(GroupDescription group, Map<PartitionId, Long> committed, Map<PartitionId, Long> latest) {
        Set<PartitionId> partitions = new HashSet<>(committed.keySet());
        group.members().forEach(member -> partitions.addAll(member.assigned()));
        List<PartitionId> sorted = partitions.stream()
                .sorted(Comparator.comparing(PartitionId::topic).thenComparingInt(PartitionId::partition))
                .toList();

        Map<String, List<PartitionLag>> byTopic = new LinkedHashMap<>();
        long totalLag = 0;
        int uncommitted = 0;
        for (PartitionId partition : sorted) {
            Long committedOffset = committed.get(partition);
            Long end = latest.get(partition);
            Long lag = committedOffset == null || end == null ? null : end - committedOffset;
            if (committedOffset == null) {
                uncommitted++;
            }
            if (lag != null) {
                totalLag += lag;
            }
            byTopic.computeIfAbsent(partition.topic(), _ -> new ArrayList<>())
                    .add(new PartitionLag(partition.partition(), committedOffset, end, lag));
        }
        List<TopicLag> topics = byTopic.entrySet().stream()
                .map(entry -> topicLag(entry.getKey(), entry.getValue()))
                .toList();
        return new ConsumerGroupDetail(group.groupId(), group.type(), group.state(), true, group.members(), topics, totalLag,
                uncommitted);
    }

    private TopicLag topicLag(String physicalTopic, List<PartitionLag> partitions) {
        List<String> virtual = virtualTopics.hasVirtualTopic(physicalTopic) ? virtualTopics.toVirtual(physicalTopic) : List.of();
        boolean filtered = virtual.stream().anyMatch(name -> virtualTopics.filterFor(name).isPresent());
        return new TopicLag(physicalTopic, virtual, filtered, partitions);
    }

    private static ConsumerGroupSummary summary(GroupSummary group, ConsumerGroupDetail detail) {
        boolean lagIsUpperBound = detail.topics().stream().anyMatch(TopicLag::filtered);
        return new ConsumerGroupSummary(detail.groupId(), detail.type(), detail.state(), group.protocolType(), detail.lagApplicable(),
                detail.totalLag(), detail.uncommittedPartitions(), lagIsUpperBound);
    }

    private static <T> T await(CompletionStage<T> stage) {
        try {
            return stage.toCompletableFuture().get(BROKER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted reading consumer groups", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("could not read consumer groups from the broker: "
                                            + (e.getCause() == null ? e.getMessage() : e.getCause().getMessage()), e);
        }
    }

    /// A group in the listing. The lag fields are zero or `false` when lag does not apply.
    ///
    /// @param groupId                the group id
    /// @param type                   the group type as the broker reports it
    /// @param state                  the group state as the broker reports it
    /// @param protocolType           the protocol type the group reports, such as `consumer` or `connect`; `null` when none
    /// @param lagApplicable          whether the group commits offsets per partition, so that lag applies
    /// @param totalLag               the lag summed over the partitions with a commit
    /// @param uncommittedPartitions  the partitions the group reads with no commit
    /// @param lagIsUpperBound        whether the group reads a physical topic that a filtered virtual topic points at, so
    ///                               records the filter drops count as lag although they are never delivered
    public record ConsumerGroupSummary(
            String groupId,
            String type,
            String state,
            @Nullable String protocolType,
            boolean lagApplicable,
            long totalLag,
            int uncommittedPartitions,
            boolean lagIsUpperBound
    ) {
    }

    /// A group with its members and the lag of each partition it reads. When lag does not apply, the group has no
    /// members or topics and zero lag.
    ///
    /// @param groupId               the group id
    /// @param type                  the group type as the broker reports it
    /// @param state                 the group state as the broker reports it
    /// @param lagApplicable         whether the group commits offsets per partition, so that lag applies
    /// @param members               the members and their assigned partitions
    /// @param topics                the physical topics the group reads, by name
    /// @param totalLag              the lag summed over the partitions with a commit and a known end offset
    /// @param uncommittedPartitions the partitions the group reads with no commit
    public record ConsumerGroupDetail(
            String groupId,
            String type,
            String state,
            boolean lagApplicable,
            List<GroupMember> members,
            List<TopicLag> topics,
            long totalLag,
            int uncommittedPartitions
    ) {
    }

    /// The lag of one physical topic that a group reads.
    ///
    /// @param physicalTopic the broker topic the offsets belong to
    /// @param virtualTopics the virtual topics that point at it, empty when none does
    /// @param filtered      whether any of those virtual topics has a filter, which makes the lag an upper bound
    /// @param partitions    the partitions the group reads, by partition number
    public record TopicLag(String physicalTopic, List<String> virtualTopics, boolean filtered, List<PartitionLag> partitions) {
    }

    /// The offsets of one partition a group reads.
    ///
    /// @param partition the partition number
    /// @param committed the offset the group committed, `null` when it has none
    /// @param end       the latest offset, `null` when the broker does not know the partition
    /// @param lag       end minus committed, `null` unless both are known
    public record PartitionLag(int partition, @Nullable Long committed, @Nullable Long end, @Nullable Long lag) {
    }
}
