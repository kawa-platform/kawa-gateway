package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GatewayConfigTest {

    @Test
    void emptyReturnsFullyDefaultedConfig() {
        // when
        GatewayConfig config = GatewayConfig.empty();

        // then
        assertThat(config.listeners()).hasSize(1);
        assertThat(config.clusters()).isEmpty();
        assertThat(config.virtualTopics()).isEmpty();
        assertThat(config.auth().mechanisms()).isEmpty();
        assertThat(config.auth().clients()).isEmpty();
        assertThat(config.rbac().roles()).isEmpty();
        assertThat(config.rbac().groups()).isEmpty();
        assertThat(config.governance().topicRules()).isEmpty();
        assertThat(config.governance().exemptions()).isEmpty();
        assertThat(config.configTopic()).isEqualTo("__kawa");
    }

    @Test
    void updateGovernanceReplacesGovernanceConfig() {
        // given
        var config = new GatewayConfig(null, null, null, null, null, null, null, null, null);
        var newGovernance = new GovernanceConfig(
                Map.of("min-partitions",
                        new GovernanceRuleConfig("must have partitions", "topic.partitions >= 1")),
                null);

        // when
        var updated = config.updateGovernance(newGovernance);

        // then
        assertThat(updated.governance()).isEqualTo(newGovernance);
    }

    @Test
    void upsertVirtualTopicAddsNewEntry() {
        // given
        var config = new GatewayConfig(null, null, null, null, null, null, null, null, null);

        // when
        var updated = config.upsertVirtualTopic("orders", new VirtualTopicConfig("raw-orders"));

        // then
        assertThat(updated.virtualTopics()).containsEntry("orders", new VirtualTopicConfig("raw-orders"));
    }

    @Test
    void upsertVirtualTopicOverwritesExisting() {
        // given
        var config = new GatewayConfig(null, null,
                Map.of("orders", new VirtualTopicConfig("raw-old")),
                null, null, null, null, null, null);

        // when
        var updated = config.upsertVirtualTopic("orders", new VirtualTopicConfig("raw-new"));

        // then
        assertThat(updated.virtualTopics()).containsEntry("orders", new VirtualTopicConfig("raw-new"));
        assertThat(updated.virtualTopics()).hasSize(1);
    }

    @Test
    void removeVirtualTopicRemovesEntry() {
        // given
        var config = new GatewayConfig(null, null,
                Map.of("orders", new VirtualTopicConfig("raw-orders")),
                null, null, null, null, null, null);

        // when
        var updated = config.removeVirtualTopic("orders");

        // then
        assertThat(updated.virtualTopics()).isEmpty();
    }

    @Test
    void updateAuthReplacesAuthConfig() {
        // given
        var config = new GatewayConfig(null, null, null, null,
                new AuthConfig(null, null, null), null, null, null, null);
        var newAuth = new AuthConfig(
                java.util.Set.of("PLAIN"),
                Map.of("alice", new ClientConfig("PLAIN",
                        HashedPassword.fromPlaintext(Mechanism.PLAIN, "secret"))),
                null);

        // when
        var updated = config.updateAuth(newAuth);

        // then
        assertThat(updated.auth()).isEqualTo(newAuth);
    }

    @Test
    void updateRbacReplacesRbacConfig() {
        // given
        var config = new GatewayConfig(null, null, null, null, null,
                new RbacConfig(null, null), null, null, null);
        var newRbac = new RbacConfig(
                Map.of("admin", new RoleConfig(java.util.List.of())),
                null);

        // when
        var updated = config.updateRbac(newRbac);

        // then
        assertThat(updated.rbac()).isEqualTo(newRbac);
    }

    @Test
    void updateVirtualTopicsReplacesMap() {
        // given
        var config = new GatewayConfig(null, null,
                Map.of("old-topic", new VirtualTopicConfig("raw-old")),
                null, null, null, null, null, null);
        var newTopics = Map.of("new-topic", new VirtualTopicConfig("raw-new"));

        // when
        var updated = config.updateVirtualTopics(newTopics);

        // then
        assertThat(updated.virtualTopics()).containsExactlyEntriesOf(newTopics);
    }

    @Test
    void upsertVirtualTopicPreservesOtherTopics() {
        // given
        var config = new GatewayConfig(null, null,
                Map.of("existing", new VirtualTopicConfig("raw-existing")),
                null, null, null, null, null, null);

        // when
        var updated = config.upsertVirtualTopic("new-topic", new VirtualTopicConfig("raw-new"));

        // then
        assertThat(updated.virtualTopics()).hasSize(2);
        assertThat(updated.virtualTopics()).containsEntry("existing", new VirtualTopicConfig("raw-existing"));
        assertThat(updated.virtualTopics()).containsEntry("new-topic", new VirtualTopicConfig("raw-new"));
    }
}
