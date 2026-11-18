package io.jonasg.kawa.http.consumergroups;

import io.jonasg.kawa.broker.BrokerAdmin.GroupDescription;
import io.jonasg.kawa.broker.BrokerAdmin.GroupMember;
import io.jonasg.kawa.broker.BrokerAdmin.PartitionId;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.http.configchange.NotFoundException;
import io.jonasg.kawa.http.consumergroups.ConsumerGroupService.ConsumerGroupDetail;
import io.jonasg.kawa.http.consumergroups.ConsumerGroupService.ConsumerGroupSummary;
import io.jonasg.kawa.http.consumergroups.ConsumerGroupService.TopicLag;
import io.jonasg.kawa.test.FakeBrokerAdmin;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.tuple;

class ConsumerGroupServiceTest {

    private final FakeBrokerAdmin brokerAdmin = new FakeBrokerAdmin();

    @Test
    void reportsLagAsEndOffsetMinusCommittedOffset() {
        // given
        var service = serviceWith(new VirtualTopicManager(Map.of()));
        groupReading("orders-consumers", "Stable", new PartitionId("orders", 0));
        commit("orders-consumers", new PartitionId("orders", 0), 40);
        end(new PartitionId("orders", 0), 100);

        // when
        ConsumerGroupDetail detail = service.describe("orders-consumers");

        // then
        assertThat(detail.totalLag())
                .withFailMessage(() -> "Group 'orders-consumers' did not report its lag as end minus committed: " + detail)
                .isEqualTo(60);
        assertThat(detail.topics().getFirst().partitions().getFirst().lag())
                .withFailMessage(() -> "Partition 0 of 'orders' did not report lag 60: " + detail)
                .isEqualTo(60L);
    }

    @Test
    void listsEveryGroupWithItsStateAndTotalLag() {
        // given
        var service = serviceWith(new VirtualTopicManager(Map.of()));
        groupReading("orders-consumers", "Stable", new PartitionId("orders", 0));
        commit("orders-consumers", new PartitionId("orders", 0), 40);
        end(new PartitionId("orders", 0), 100);
        groupReading("audit", "Empty");

        // when
        List<ConsumerGroupSummary> groups = service.list();

        // then
        assertThat(groups)
                .withFailMessage(() -> "The listing did not show each group with its state and total lag: " + groups)
                .extracting(ConsumerGroupSummary::groupId, ConsumerGroupSummary::state, ConsumerGroupSummary::totalLag)
                .containsExactly(tuple("orders-consumers", "Stable", 60L), tuple("audit", "Empty", 0L));
    }

    @Test
    void showsAssignedPartitionWithoutCommitAsUncommittedInsteadOfZeroLag() {
        // given
        var service = serviceWith(new VirtualTopicManager(Map.of()));
        groupReading("orders-consumers", "Stable", new PartitionId("orders", 0), new PartitionId("orders", 1));
        commit("orders-consumers", new PartitionId("orders", 0), 40);
        end(new PartitionId("orders", 0), 100);
        end(new PartitionId("orders", 1), 70);

        // when
        ConsumerGroupDetail detail = service.describe("orders-consumers");

        // then
        var withoutCommit = detail.topics().getFirst().partitions().get(1);
        assertThat(withoutCommit.committed())
                .withFailMessage(() -> "Partition 1 with no commit was not shown without a committed offset: " + withoutCommit)
                .isNull();
        assertThat(withoutCommit.lag())
                .withFailMessage(() -> "Partition 1 with no commit was shown with a lag instead of none: " + withoutCommit)
                .isNull();
        assertThat(detail.uncommittedPartitions())
                .withFailMessage(() -> "The one partition without a commit was not counted: " + detail)
                .isEqualTo(1);
        assertThat(detail.totalLag())
                .withFailMessage(() -> "Total lag counted the partition without a commit: " + detail)
                .isEqualTo(60);
    }

    @Test
    void reportsLagOnThePhysicalTopicUnderItsVirtualTopic() {
        // given
        var service = serviceWith(new VirtualTopicManager(Map.of("orders", new VirtualTopicConfig("orders-v2"))));
        groupReading("orders-consumers", "Stable", new PartitionId("orders-v2", 0));
        commit("orders-consumers", new PartitionId("orders-v2", 0), 40);
        end(new PartitionId("orders-v2", 0), 100);

        // when
        ConsumerGroupDetail detail = service.describe("orders-consumers");

        // then
        var topic = detail.topics().getFirst();
        assertThat(topic.physicalTopic())
                .withFailMessage(() -> "Lag was not reported against the physical topic 'orders-v2': " + detail)
                .isEqualTo("orders-v2");
        assertThat(topic.virtualTopics())
                .withFailMessage(() -> "The virtual topic 'orders' on 'orders-v2' was not named: " + topic)
                .containsExactly("orders");
        assertThat(topic.filtered())
                .withFailMessage(() -> "An unfiltered virtual topic was marked as an upper bound: " + topic)
                .isFalse();
    }

    @Test
    void marksLagAsUpperBoundWhenTheVirtualTopicOnThePhysicalTopicFilters() {
        // given
        var filter = new HeaderEqualsFilterConfig("type", "order");
        var service = serviceWith(new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2", filter))));
        groupReading("orders-consumers", "Stable", new PartitionId("orders-v2", 0));
        commit("orders-consumers", new PartitionId("orders-v2", 0), 40);
        end(new PartitionId("orders-v2", 0), 100);

        // when
        ConsumerGroupDetail detail = service.describe("orders-consumers");

        // then
        assertThat(detail.topics().getFirst().filtered())
                .withFailMessage(() -> "Lag through a filtered virtual topic was not marked as an upper bound: " + detail)
                .isTrue();
    }

    @Test
    void refusesToDescribeAGroupTheBrokerDoesNotKnow() {
        // given
        var service = serviceWith(new VirtualTopicManager(Map.of()));
        groupReading("orders-consumers", "Stable", new PartitionId("orders", 0));

        // when
        var thrown = catchThrowable(() -> service.describe("orders-missing"));

        // then
        assertThat(thrown)
                .withFailMessage(() -> "Describing a group the broker does not know did not fail naming it: " + thrown)
                .isInstanceOf(NotFoundException.class)
                .hasMessageContaining("orders-missing");
    }

    @Test
    void reportsAnAssignmentMadeUnderAVirtualTopicOnItsPhysicalTopic() {
        // given: the broker holds the member's assignment under the virtual name the client was given
        var service = serviceWith(new VirtualTopicManager(Map.of("orders", new VirtualTopicConfig("orders-v2"))));
        groupReading("orders-consumers", "Stable", new PartitionId("orders", 0));
        commit("orders-consumers", new PartitionId("orders-v2", 0), 40);
        end(new PartitionId("orders-v2", 0), 100);

        // when
        ConsumerGroupDetail detail = service.describe("orders-consumers");

        // then
        assertThat(detail.topics())
                .withFailMessage(() -> "The assignment under the virtual name was reported as a topic of its own: " + detail)
                .extracting(TopicLag::physicalTopic)
                .containsExactly("orders-v2");
        assertThat(detail.topics().getFirst().partitions().getFirst().lag())
                .withFailMessage(() -> "The assigned partition did not show the lag of its physical partition: " + detail)
                .isEqualTo(60L);
        assertThat(detail.uncommittedPartitions())
                .withFailMessage(() -> "The assigned partition was counted as uncommitted under its virtual name: " + detail)
                .isZero();
    }

    @Test
    void refusesToDescribeADeadGroupWithNoMembersAndNoCommits() {
        // given
        var service = serviceWith(new VirtualTopicManager(Map.of()));
        brokerAdmin.groups.put("orders-gone", new GroupDescription("orders-gone", "CLASSIC", "Dead", List.of()));

        // when
        var thrown = catchThrowable(() -> service.describe("orders-gone"));

        // then
        assertThat(thrown)
                .withFailMessage(() -> "A dead group with nothing left was described instead of reported unknown: " + thrown)
                .isInstanceOf(NotFoundException.class)
                .hasMessageContaining("orders-gone");
    }

    @Test
    void listsShareAndStreamsGroupsWithLagNotApplicable() {
        // given
        var service = serviceWith(new VirtualTopicManager(Map.of()));
        brokerAdmin.groups.put("orders-share", new GroupDescription("orders-share", "SHARE", "Stable", List.of()));
        brokerAdmin.groups.put("orders-streams", new GroupDescription("orders-streams", "STREAMS", "Stable", List.of()));

        // when
        List<ConsumerGroupSummary> groups = service.list();

        // then
        assertThat(groups)
                .withFailMessage(() -> "The share and streams groups were not listed with their type and state: " + groups)
                .extracting(ConsumerGroupSummary::groupId, ConsumerGroupSummary::type, ConsumerGroupSummary::state)
                .containsExactly(tuple("orders-share", "SHARE", "Stable"), tuple("orders-streams", "STREAMS", "Stable"));
        assertThat(groups)
                .withFailMessage(() -> "The share and streams groups were reported as having lag that applies: " + groups)
                .extracting(ConsumerGroupSummary::groupId, ConsumerGroupSummary::lagApplicable)
                .containsExactly(tuple("orders-share", false), tuple("orders-streams", false));
    }

    @Test
    void listsAClassicGroupOfAnotherProtocolWithLagNotApplicable() {
        // given
        var service = serviceWith(new VirtualTopicManager(Map.of()));
        brokerAdmin.groups.put("connect-sink", new GroupDescription("connect-sink", "CLASSIC", "Stable", List.of()));
        brokerAdmin.protocolTypes.put("connect-sink", "connect");

        // when
        List<ConsumerGroupSummary> groups = service.list();

        // then
        assertThat(groups)
                .withFailMessage(() -> "The classic group of protocol 'connect' was not listed with its type and state: " + groups)
                .extracting(ConsumerGroupSummary::groupId, ConsumerGroupSummary::type, ConsumerGroupSummary::state)
                .containsExactly(tuple("connect-sink", "CLASSIC", "Stable"));
        assertThat(groups)
                .withFailMessage(() -> "The connect group was reported as having lag that applies: " + groups)
                .extracting(ConsumerGroupSummary::lagApplicable)
                .containsExactly(false);
    }

    @Test
    void listsTheLagOfConsumerGroupsWhenTheClusterAlsoHasAShareGroup() {
        // given
        var service = serviceWith(new VirtualTopicManager(Map.of()));
        groupReading("orders-consumers", "Stable", new PartitionId("orders", 0));
        commit("orders-consumers", new PartitionId("orders", 0), 40);
        end(new PartitionId("orders", 0), 100);
        brokerAdmin.groups.put("orders-share", new GroupDescription("orders-share", "SHARE", "Stable", List.of()));

        // when
        List<ConsumerGroupSummary> groups = service.list();

        // then
        assertThat(groups)
                .withFailMessage(() -> "The share group hid the lag of the consumer group next to it: " + groups)
                .extracting(ConsumerGroupSummary::groupId, ConsumerGroupSummary::totalLag)
                .contains(tuple("orders-consumers", 60L));
        assertThat(groups)
                .withFailMessage(() -> "The consumer group was not reported as having lag that applies: " + groups)
                .extracting(ConsumerGroupSummary::groupId, ConsumerGroupSummary::lagApplicable)
                .contains(tuple("orders-consumers", true));
        assertThat(groups)
                .withFailMessage(() -> "The share group was left out of the listing: " + groups)
                .extracting(ConsumerGroupSummary::groupId)
                .contains("orders-share");
    }

    @Test
    void describesAShareGroupWithItsStateAndLagNotApplicable() {
        // given
        var service = serviceWith(new VirtualTopicManager(Map.of()));
        brokerAdmin.groups.put("orders-share", new GroupDescription("orders-share", "SHARE", "Stable", List.of()));

        // when
        ConsumerGroupDetail detail = service.describe("orders-share");

        // then
        assertThat(detail.type())
                .withFailMessage(() -> "The share group was not described with its type: " + detail)
                .isEqualTo("SHARE");
        assertThat(detail.state())
                .withFailMessage(() -> "The share group was not described with its state: " + detail)
                .isEqualTo("Stable");
        assertThat(detail.lagApplicable())
                .withFailMessage(() -> "The share group was described as having lag that applies: " + detail)
                .isFalse();
    }

    private ConsumerGroupService serviceWith(VirtualTopicManager virtualTopics) {
        return new ConsumerGroupService(virtualTopics, brokerAdmin);
    }

    /// Adds a group with one member assigned `partitions`.
    private void groupReading(String groupId, String state, PartitionId... partitions) {
        var member = new GroupMember("member-1", "client-1", "/10.0.0.1", List.of(partitions));
        brokerAdmin.groups.put(groupId, new GroupDescription(groupId, "CLASSIC", state, List.of(member)));
    }

    private void commit(String groupId, PartitionId partition, long offset) {
        brokerAdmin.committed.computeIfAbsent(groupId, _ -> new HashMap<>()).put(partition, offset);
    }

    private void end(PartitionId partition, long offset) {
        brokerAdmin.latest.put(partition, offset);
    }
}
