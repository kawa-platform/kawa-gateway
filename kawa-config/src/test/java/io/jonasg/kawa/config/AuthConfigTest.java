package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuthConfigTest {

    private static ClientConfig client(String mechanism, String password) {
        Mechanism m = mechanism == null ? Mechanism.PLAIN : Mechanism.fromWireName(mechanism);
        return new ClientConfig(mechanism, HashedPassword.fromPlaintext(m, password));
    }

    @Test
    void clientWithoutMechanismInheritsGlobalMechanism() {
        // given
        Map<String, ClientConfig> clients = Map.of("alice", client(null, "secret"));

        // when
        AuthConfig config = new AuthConfig(Set.of("PLAIN"), clients, null);

        // then
        assertThat(config.clients().get("alice").mechanism()).isEqualTo("PLAIN");
    }

    @Test
    void clientWithoutMechanismFailsWhenNoGlobalMechanismIsConfigured() {
        // given
        Map<String, ClientConfig> clients = Map.of("alice", client(null, "secret"));

        // when / then
        assertThatThrownBy(() -> new AuthConfig(Set.of(), clients, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("alice")
                .hasMessageContaining("mechanism");
    }

    @Test
    void explicitClientMechanismOverridesGlobalMechanism() {
        // given
        Map<String, ClientConfig> clients = Map.of("bob", client("SCRAM-SHA-256", "secret"));

        // when
        AuthConfig config = new AuthConfig(Set.of("PLAIN", "SCRAM-SHA-256"), clients, null);

        // then
        assertThat(config.clients().get("bob").mechanism()).isEqualTo("SCRAM-SHA-256");
    }

    @Test
    void rejectsClientMechanismNotInAdvertisedList() {
        // given
        Map<String, ClientConfig> clients = Map.of(
                "bob", client("SCRAM-SHA-256", "secret"));

        // when / then
        assertThatThrownBy(() -> new AuthConfig(Set.of("PLAIN"), clients, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bob")
                .hasMessageContaining("SCRAM-SHA-256")
                .hasMessageContaining("PLAIN");
    }

    @Test
    void upsertClientAddsNewClient() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"), Map.of(), null);

        // when
        var updated = config.upsertClient("alice", client("PLAIN", "secret"));

        // then
        assertThat(updated.clients()).containsKey("alice");
        assertThat(updated.clients().get("alice").mechanism()).isEqualTo("PLAIN");
    }

    @Test
    void upsertClientOverwritesExisting() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"),
                Map.of("alice", client("PLAIN", "old-secret")), null);

        // when
        var updated = config.upsertClient("alice", client("PLAIN", "new-secret"));

        // then
        assertThat(updated.clients()).hasSize(1);
        assertThat(updated.clients().get("alice").password().verify("new-secret"))
                .withFailMessage(() -> "Upserted client password did not verify against the new plaintext")
                .isTrue();
    }

    @Test
    void removeClientRemovesExisting() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"),
                Map.of("alice", client("PLAIN", "secret")), null);

        // when
        var updated = config.removeClient("alice");

        // then
        assertThat(updated.clients()).isEmpty();
    }

    @Test
    void removeClientPreservesOtherClients() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"), Map.of(
                "alice", client("PLAIN", "secret-alice"),
                "bob", client("PLAIN", "secret-bob")), null);

        // when
        var updated = config.removeClient("alice");

        // then
        assertThat(updated.clients()).hasSize(1);
        assertThat(updated.clients()).containsKey("bob");
    }

    @Test
    void upsertClientPreservesMechanisms() {
        // given
        var config = new AuthConfig(Set.of("PLAIN", "SCRAM-SHA-256"), Map.of(), null);

        // when
        var updated = config.upsertClient("alice", client("PLAIN", "secret"));

        // then
        assertThat(updated.mechanisms()).containsExactlyInAnyOrder("PLAIN", "SCRAM-SHA-256");
    }

    @Test
    void upsertClientAddsMechanismWhenClientUsesNewOne() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"), Map.of(), null);

        // when
        var updated = config.upsertClient("bob", client("SCRAM-SHA-256", "secret"));

        // then
        assertThat(updated.mechanisms()).containsExactlyInAnyOrder("PLAIN", "SCRAM-SHA-256");
        assertThat(updated.clients().get("bob").mechanism()).isEqualTo("SCRAM-SHA-256");
    }

    @Test
    void upsertClientWithoutMechanismIsRejected() {
        // given
        var config = new AuthConfig(Set.of(), Map.of(), null);

        // when / then
        assertThatThrownBy(() -> config.upsertClient("alice", client(null, "secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("alice")
                .hasMessageContaining("mechanism");
    }

    @Test
    void upsertClientWithoutMechanismIsRejectedEvenWhenMechanismsConfigured() {
        // given
        var config = new AuthConfig(Set.of("SCRAM-SHA-256", "PLAIN"), Map.of(), null);

        // when / then
        assertThatThrownBy(() -> config.upsertClient("alice", client(null, "secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("alice")
                .hasMessageContaining("mechanism");
    }

    @Test
    void upsertClientRejectsNullClient() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"), Map.of(), null);

        // when / then
        assertThatThrownBy(() -> config.upsertClient("alice", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("alice");
    }

    @Test
    void brokerAuthPreservedThroughWithClient() {
        // given
        var brokerAuth = BrokerAuthConfig.of("PLAIN", "admin", "kafka-secret", k -> null);
        var config = new AuthConfig(Set.of("PLAIN"),
                Map.of("alice", client("PLAIN", "secret")), brokerAuth);

        // when
        var updated = config.upsertClient("bob", client("PLAIN", "bob-secret"));

        // then
        assertThat(updated.brokerAuth()).isEqualTo(brokerAuth);
        assertThat(updated.clients()).hasSize(2);
    }
}
