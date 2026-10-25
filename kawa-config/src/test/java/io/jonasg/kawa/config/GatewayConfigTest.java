package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GatewayConfigTest {

    private static final String ALICE_ID = "0b6f7e0a-2a4c-4e8e-9d6b-1f2a3b4c5d6e";

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
        var user = new AdminUserConfig("alice@mycom.com", HashedPassword.fromPlaintext(Mechanism.PLAIN, "s3cret"),
                null, true, Instant.parse("2026-10-06T12:00:00Z"));

        // when
        var added = GatewayConfig.empty().upsertAdminUser(ALICE_ID, user);
        var removed = added.removeAdminUser(ALICE_ID);

        // then
        assertThat(added.adminUsers())
                .withFailMessage(() -> "admin user 'alice@mycom.com' was not added: " + added.adminUsers())
                .containsEntry(ALICE_ID, user);
        assertThat(removed.adminUsers())
                .withFailMessage(() -> "admin user 'alice@mycom.com' was not removed: " + removed.adminUsers())
                .isEmpty();
    }

    @Test
    void adminUsersSurviveAJsonRoundTripWithTheirHashOnly() {
        // given
        var mapper = JsonMapper.builder().build();
        var user = new AdminUserConfig("alice@mycom.com", HashedPassword.fromPlaintext(Mechanism.PLAIN, "s3cret"),
                "Alice Liddell", true, Instant.parse("2026-10-06T12:00:00Z"));
        var config = GatewayConfig.empty().upsertAdminUser(ALICE_ID, user);

        // when
        String json = mapper.writeValueAsString(config);
        GatewayConfig restored = mapper.readValue(json, GatewayConfig.class);

        // then
        assertThat(json)
                .withFailMessage(() -> "the plaintext admin password was serialized: " + json)
                .doesNotContain("s3cret");
        assertThat(restored.adminUsers().get(ALICE_ID))
                .withFailMessage(() -> "admin user 'alice@mycom.com' changed in the round trip: " + restored.adminUsers())
                .isEqualTo(user);
        assertThat(restored.adminUsers().get(ALICE_ID).password().verify("s3cret"))
                .withFailMessage(() -> "admin user 'alice@mycom.com' did not verify after the round trip")
                .isTrue();
    }

    @Test
    void managedByMarkersSurviveAJsonRoundTrip() {
        // given
        var mapper = JsonMapper.builder().build();
        var operator = new ManagedBy("kawa-operator");
        var config = managedEntries(operator);

        // when
        GatewayConfig restored = mapper.readValue(mapper.writeValueAsString(config), GatewayConfig.class);

        // then
        assertThat(restored.virtualTopics().get("orders").managedBy())
                .withFailMessage(() -> "virtual topic 'orders' lost its owner in the round trip: " + restored.virtualTopics())
                .isEqualTo(operator);
        assertThat(restored.rbac().roles().get("reader").managedBy())
                .withFailMessage(() -> "role 'reader' lost its owner in the round trip: " + restored.rbac().roles())
                .isEqualTo(operator);
        assertThat(restored.rbac().groups().get("producers").managedBy())
                .withFailMessage(() -> "group 'producers' lost its owner in the round trip: " + restored.rbac().groups())
                .isEqualTo(operator);
        assertThat(restored.auth().clients().get("alice").managedBy())
                .withFailMessage(() -> "client 'alice' lost its owner in the round trip: " + restored.auth().clients())
                .isEqualTo(operator);
    }

    @Test
    void aSnapshotWrittenBeforeManagedByLoadsAsUnmanaged() {
        // given: a snapshot of the shape written before entries carried an owner
        var mapper = JsonMapper.builder().build();
        String json = mapper.writeValueAsString(managedEntries(null)).replace(",\"managedBy\":null", "");

        // when
        GatewayConfig restored = mapper.readValue(json, GatewayConfig.class);

        // then
        assertThat(json)
                .withFailMessage(() -> "the snapshot still names managedBy: " + json)
                .doesNotContain("managedBy");
        assertThat(restored.virtualTopics().get("orders").managedBy())
                .withFailMessage(() -> "virtual topic 'orders' from an old snapshot was not unmanaged")
                .isNull();
        assertThat(restored.rbac().roles().get("reader").managedBy())
                .withFailMessage(() -> "role 'reader' from an old snapshot was not unmanaged")
                .isNull();
        assertThat(restored.rbac().groups().get("producers"))
                .withFailMessage(() -> "group 'producers' from an old snapshot did not load unmanaged: " + restored.rbac().groups())
                .isEqualTo(new GroupConfig(List.of("alice"), List.of("reader")));
        assertThat(restored.auth().clients().get("alice").managedBy())
                .withFailMessage(() -> "client 'alice' from an old snapshot was not unmanaged")
                .isNull();
    }

    @Test
    void removingARolePreservesTheOwnerOfTheGroupsItIsStrippedFrom() {
        // given
        var operator = new ManagedBy("kawa-operator");
        var rbac = new RbacConfig()
                .upsertRole("reader", new RoleConfig(List.of()))
                .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of("reader"), operator));

        // when
        var updated = rbac.removeRole("reader");

        // then
        assertThat(updated.groups().get("producers"))
                .withFailMessage(() -> "group 'producers' lost its owner or kept the removed role: " + updated.groups())
                .isEqualTo(new GroupConfig(List.of("alice"), List.of(), operator));
    }

    private static GatewayConfig managedEntries(ManagedBy managedBy) {
        var config = GatewayConfig.empty()
                .upsertVirtualTopic("orders", new VirtualTopicConfig("orders-v2", null, false, null, managedBy));
        return config
                .updateRbac(config.rbac()
                        .upsertRole("reader", new RoleConfig(List.of(), managedBy))
                        .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of("reader"), managedBy)))
                .updateAuth(config.auth().upsertClient("alice", ClientConfig.fromPlaintext("PLAIN", "secret").withManagedBy(managedBy)));
    }

    @Test
    void disabledAdminUserStaysDisabledAfterRoundTrip() {
        // given
        var mapper = JsonMapper.builder().build();
        var user = new AdminUserConfig("alice@mycom.com", HashedPassword.fromPlaintext(Mechanism.PLAIN, "s3cret"),
                null, false, Instant.parse("2026-10-06T12:00:00Z"));
        var config = GatewayConfig.empty().upsertAdminUser(ALICE_ID, user);

        // when
        GatewayConfig restored = mapper.readValue(mapper.writeValueAsString(config), GatewayConfig.class);

        // then
        assertThat(restored.adminUsers().get(ALICE_ID).enabled())
                .withFailMessage(() -> "disabled admin user 'alice@mycom.com' was enabled after the round trip")
                .isFalse();
    }

    @Test
    void modulesSurviveAJsonRoundTripUnderTheirId() {
        // given
        var mapper = JsonMapper.builder().build();
        var config = GatewayConfig.empty()
                .upsertModule(new ModuleConfig(ModuleId.VIRTUAL_TOPICS, false))
                .upsertModule(new ModuleConfig(ModuleId.RBAC, true));

        // when
        String json = mapper.writeValueAsString(config);
        GatewayConfig restored = mapper.readValue(json, GatewayConfig.class);

        // then
        assertThat(json)
                .withFailMessage(() -> "module 'virtual-topics' was not stored under its id: " + json)
                .contains("\"virtual-topics\":{\"id\":\"virtual-topics\",\"enabled\":false}");
        assertThat(restored.modules())
                .withFailMessage(() -> "modules changed in the round trip: " + restored.modules())
                .isEqualTo(config.modules());
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
