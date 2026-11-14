package io.jonasg.kawa.http.consumergroups;

import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.http.topics.TopicAdmin;
import io.jonasg.kawa.http.topics.TopicAdmin.GroupDetails;
import io.jonasg.kawa.http.topics.TopicAdmin.GroupMember;
import io.jonasg.kawa.http.topics.TopicAdmin.GroupPartition;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/// Reads the broker's consumer groups with their committed and end offsets, through the
/// [TopicAdmin]. Offsets are those of the physical topics: the broker knows nothing of virtual
/// topics. Every read is a fixed number of batched broker calls, however many groups there are.
/// Does not own the [TopicAdmin]; [io.jonasg.kawa.http.topics.TopicService] closes it.
public final class ConsumerGroupService {

    private static final Comparator<GroupPartition> BY_PARTITION =
            Comparator.comparing(GroupPartition::topic).thenComparingInt(GroupPartition::partition);

    private final TopicAdmin topicAdmin;

    public ConsumerGroupService(TopicAdmin topicAdmin) {
        this.topicAdmin = topicAdmin;
    }

    /// Every consumer group with the offsets of the partitions it has committed, sorted by group id.
    public List<ConsumerGroupOffsets> list() {
        return read("list consumer groups", () -> {
            var groups = topicAdmin.listConsumerGroups();
            var committed = topicAdmin.committedOffsets(groups.stream().map(TopicAdmin.GroupSummary::groupId).toList());
            var ends = topicAdmin.endOffsets(committed.values().stream()
                    .flatMap(offsets -> offsets.keySet().stream())
                    .collect(Collectors.toSet()));
            return groups.stream()
                    .sorted(Comparator.comparing(TopicAdmin.GroupSummary::groupId))
                    .map(group -> {
                        var offsets = committed.getOrDefault(group.groupId(), Map.of());
                        return new ConsumerGroupOffsets(group.groupId(), group.state(), List.of(),
                                partitions(offsets.keySet(), offsets, ends));
                    })
                    .toList();
        });
    }

    /// The group `groupId` with its members and the offsets of every partition it is assigned or
    /// has committed; an assigned partition without a commit has no committed offset.
    ///
    /// @throws NotFoundException when the broker does not know the group
    public ConsumerGroupOffsets describe(String groupId) {
        return read("describe consumer group '" + groupId + "'", () -> {
            GroupDetails group = topicAdmin.describeConsumerGroups(List.of(groupId)).get(groupId);
            if (group == null) {
                throw new NotFoundException("consumer group '" + groupId + "' not found");
            }
            var committed = topicAdmin.committedOffsets(List.of(groupId)).getOrDefault(groupId, Map.of());
            Set<GroupPartition> read = new TreeSet<>(BY_PARTITION);
            read.addAll(committed.keySet());
            group.members().forEach(member -> read.addAll(member.assignment()));
            var ends = topicAdmin.endOffsets(read);
            return new ConsumerGroupOffsets(group.groupId(), group.state(), group.members(), partitions(read, committed, ends));
        });
    }

    private static List<PartitionOffsets> partitions(
            Collection<GroupPartition> partitions,
            Map<GroupPartition, Long> committed,
            Map<GroupPartition, Long> ends
    ) {
        return partitions.stream()
                .sorted(BY_PARTITION)
                .map(p -> new PartitionOffsets(p.topic(), p.partition(), committed.get(p), ends.get(p)))
                .toList();
    }

    private static <T> T read(String action, BrokerRead<T> read) {
        try {
            return read.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted trying to " + action, e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("failed to " + action + ": "
                    + (e.getCause() == null ? e.getMessage() : e.getCause().getMessage()), e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("failed to " + action + ": " + e.getMessage(), e);
        }
    }

    @FunctionalInterface
    private interface BrokerRead<T> {
        T get() throws Exception;
    }

    /// A consumer group and the offsets of the partitions it reads.
    ///
    /// @param groupId    the group's id
    /// @param state      the group's state as the broker names it, e.g. `Stable` or `Empty`
    /// @param members    the group's members; empty when only listed
    /// @param partitions the partitions it reads, sorted by topic and partition
    public record ConsumerGroupOffsets(String groupId, String state, List<GroupMember> members, List<PartitionOffsets> partitions) {
    }

    /// The offsets of one physical partition for a group.
    ///
    /// @param topic           the physical topic
    /// @param partition       the partition number
    /// @param committedOffset the group's committed offset, or `null` when it has committed none
    /// @param endOffset       the partition's end offset, or `null` when the broker does not know the partition
    public record PartitionOffsets(String topic, int partition, @Nullable Long committedOffset, @Nullable Long endOffset) {
    }
}
