package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

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
        assertThat(config.governance().rules()).isEmpty();
        assertThat(config.configTopic()).isEqualTo("__kawa");
        assertThat(config.adminUsers()).isEmpty();
    }

    @Test
    void upsertAdminUserAddsAndRemoveAdminUserRemoves() {
        // given
        var user = AdminUserConfig.fromPlaintext("s3cret");

        // when
        var added = GatewayConfig.empty().upsertAdminUser("alice", user);
        var removed = added.removeAdminUser("alice");

        // then
        assertThat(added.adminUsers())
                .withFailMessage(() -> "admin user 'alice' was not added: " + added.adminUsers())
                .containsEntry("alice", user);
        assertThat(removed.adminUsers())
                .withFailMessage(() -> "admin user 'alice' was not removed: " + removed.adminUsers())
                .isEmpty();
    }

    @Test
    void adminUsersSurviveAJsonRoundTripWithTheirHashOnly() {
        // given
        var mapper = JsonMapper.builder().build();
        var config = GatewayConfig.empty().upsertAdminUser("alice", AdminUserConfig.fromPlaintext("s3cret"));

        // when
        String json = mapper.writeValueAsString(config);
        GatewayConfig restored = mapper.readValue(json, GatewayConfig.class);

        // then
        assertThat(json)
                .withFailMessage(() -> "the plaintext admin password was serialized: " + json)
                .doesNotContain("s3cret");
        assertThat(restored.adminUsers().get("alice").password().verify("s3cret"))
                .withFailMessage(() -> "admin user 'alice' did not verify after the round trip")
                .isTrue();
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
