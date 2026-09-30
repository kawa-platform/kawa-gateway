package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceExemptionConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig.Expression;
import io.jonasg.kawa.config.GovernanceRuleConfig.Selector;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataSnapshot;
import io.jonasg.kawa.core.cluster.PartitionMetadata;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.virtualtopic.VirtualTopicManager;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicServiceTest {

    private final VirtualTopicManager virtualTopics = new VirtualTopicManager(Map.of());
    private final MetadataCache cache = new MetadataCache();
    private final FakeGatewayConfigRepository repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
    private final FakeTopicAdmin topicAdmin = new FakeTopicAdmin();
    private GovernancePolicy governance = new GovernancePolicy(new GovernanceConfig(null, null));
    private final TopicService service = new TopicService(virtualTopics, cache, repository, topicAdmin, governance);

    @Test
    void listsVirtualAndPhysicalTopics() {
        // given
        var virtualTopicsWithOrders = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2")));
        var serviceWithVirtual = new TopicService(virtualTopicsWithOrders, cache, repository, topicAdmin, governance);
        cacheWith(topic("orders-v2", 3, 2));

        // when
        var listing = serviceWithVirtual.listTopics();

        // then
        assertThat(listing.virtual()).extracting(TopicService.VirtualTopicEntry::name).containsExactly("orders");
        assertThat(listing.physical()).hasSize(1);
    }

    @Test
    void putsVirtualTopic() {
        // given
        var request = new TopicRequest("virtual", "orders", null, null, null, "orders-v2", null, null, null);

        // when
        VirtualTopicConfig result = service.upsertVirtualTopic("orders", request, Consistency.PERSISTED);

        // then
        assertThat(result.topic()).isEqualTo("orders-v2");
        assertThat(repository.getActiveConfig().virtualTopics()).containsKey("orders");
    }

    @Test
    void patchesVirtualTopic() {
        // given
        repository.update(base -> base.upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v1")));
        var patch = new VirtualTopicConfigPatch(null, "orders-v2", null, null, null);

        // when
        VirtualTopicConfig result = service.updateVirtualTopic("orders", patch, Consistency.PERSISTED);

        // then
        assertThat(result.topic()).isEqualTo("orders-v2");
        assertThat(repository.getActiveConfig().virtualTopics().get("orders").topic()).isEqualTo("orders-v2");
    }

    @Test
    void patchesVirtualTopicRename() {
        // given
        repository.update(base -> base.upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v1")));
        var patch = new VirtualTopicConfigPatch("regional-orders", null, null, null, null);

        // when
        VirtualTopicConfig result = service.updateVirtualTopic("orders", patch, Consistency.PERSISTED);

        // then
        assertThat(result.topic()).isEqualTo("orders-v1");
        assertThat(repository.getActiveConfig().virtualTopics()).containsKey("regional-orders");
        assertThat(repository.getActiveConfig().virtualTopics()).doesNotContainKey("orders");
    }

    @Test
    void deletesVirtualTopic() {
        // given
        repository.update(base -> base.upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v2")));

        // when
        service.deleteTopic("orders", Consistency.PERSISTED);

        // then
        assertThat(repository.getActiveConfig().virtualTopics()).isEmpty();
        assertThat(topicAdmin.deleted).isEmpty();
    }

    @Test
    void deletesPhysicalTopic() {
        // given
        cacheWith(topic("orders", 1, 1));

        // when
        service.deleteTopic("orders", Consistency.PERSISTED);

        // then
        assertThat(topicAdmin.deleted).containsExactly("orders");
    }

    @Test
    void rejectsPatchForPhysicalTopic() {
        // given
        cacheWith(topic("orders", 1, 1));

        // then
        assertThatThrownBy(() -> service.updateVirtualTopic(
                "orders", new VirtualTopicConfigPatch(null, "x", null, null, null), Consistency.PERSISTED))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only virtual topics can be patched");
    }

    @Test
    void createsPhysicalTopic() throws Exception {
        // given
        var request = new TopicRequest(
                "physical", "orders", 3, (short) 3, Map.of("cleanup.policy", "compact"), null, null, null, null);

        // when
        var spec = service.createPhysicalTopic(request);

        // then
        assertThat(spec.name()).isEqualTo("orders");
        assertThat(topicAdmin.created).hasSize(1);
    }

    @Test
    void rejectsPhysicalTopicViolatingGovernance() {
        // given
        governance = new GovernancePolicy(new GovernanceConfig(
                Map.of("min-replication",
                        minReplicationRule()),
                Map.of()));
        TopicService governedService = new TopicService(virtualTopics, cache, repository, topicAdmin, governance);
        var request = new TopicRequest("physical", "orders", 3, (short) 1, null, null, null, null, null);

        // then
        assertThatThrownBy(() -> governedService.createPhysicalTopic(request))
                .isInstanceOf(ForbiddenException.class)
                .hasMessageContaining("rejected by governance");
        assertThat(topicAdmin.created).isEmpty();
    }

    @Test
    void acceptsExemptTopicAndCreatesOnBroker() throws Exception {
        // given
        governance = new GovernancePolicy(new GovernanceConfig(
                Map.of("min-replication",
                        minReplicationRule()),
                Map.of("ops", new GovernanceExemptionConfig("admin", ".*"))));
        TopicService governedService = new TopicService(virtualTopics, cache, repository, topicAdmin, governance);
        var request = new TopicRequest("physical", "orders", 3, (short) 1, null, null, null, null, null);

        // when
        var spec = governedService.createPhysicalTopic(request);

        // then
        assertThat(spec.name()).isEqualTo("orders");
        assertThat(topicAdmin.created).hasSize(1);
    }

    @Test
    void throwsConflictExceptionWhenPhysicalTopicExists() {
        // given
        topicAdmin.createError = new TopicExistsException("Topic 'orders' already exists.");
        var request = new TopicRequest("physical", "orders", 3, (short) 3, null, null, null, null, null);

        // then
        assertThatThrownBy(() -> service.createPhysicalTopic(request))
                .isInstanceOf(ConflictException.class)
                .hasMessageContaining("already exists");
    }

    private static GovernanceRuleConfig minReplicationRule() {
        return new GovernanceRuleConfig(
                "min-replication",
                "replication factor must be at least 3",
                "Topics need at least 3 replicas to survive a broker loss.",
                Selector.topic(Expression.cel("true")),
                Expression.cel("topic.replicationFactor >= 3"));
    }

    private void cacheWith(TopicMetadata... topics) {
        Map<String, TopicMetadata> topicMap = new java.util.HashMap<>();
        for (TopicMetadata topic : topics) {
            topicMap.put(topic.name(), topic);
        }
        cache.update(MetadataSnapshot.of(
                topicMap,
                Map.of(1, BrokerNode.of(1, "localhost", 9092, null)),
                "test-cluster"));
    }

    private static TopicMetadata topic(String name, int partitions, int replicas) {
        var partitionList = new java.util.ArrayList<PartitionMetadata>();
        for (int i = 0; i < partitions; i++) {
            partitionList.add(PartitionMetadata.of(
                    i, 1, Collections.nCopies(replicas, 1), List.of(1), List.of()));
        }
        return TopicMetadata.of(name, partitionList);
    }
}
