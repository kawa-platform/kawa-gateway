package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.Mechanism;
import org.junit.jupiter.api.Test;

import java.util.List;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

/// Slice tests for the `/auth/clients` admin surface: real HTTP requests through a booted
/// [AdminHttpServer], asserting the JSON wire format the admin UI consumes.
class ClientSliceTest extends AdminHttpSliceTestBase {

    private static ClientConfig client(String mechanism, String password) {
        return new ClientConfig(mechanism,
                HashedPassword.fromPlaintext(Mechanism.fromWireName(mechanism), password));
    }

    @Test
    void listsConfiguredClients() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().upsertClient("alice", client("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("GET", "/auth/clients", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                [{"username":"alice","mechanism":"PLAIN"}]
                """);
    }

    @Test
    void listsClientsSortedByUsername() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth()
                        .upsertClient("bob", client("PLAIN", "bob-secret"))
                        .upsertClient("alice", client("PLAIN", "alice-secret"))));
        startServer();

        // when
        var response = send("GET", "/auth/clients", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                [
                  {"username":"alice","mechanism":"PLAIN"},
                  {"username":"bob","mechanism":"PLAIN"}
                ]
                """);
    }

    @Test
    void listsClientsEmptyWhenNoSnapshotApplied() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(null);
        startServer();

        // when
        var response = send("GET", "/auth/clients", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("[]");
    }

    @Test
    void addsClientAndPersistsSnapshot() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/clients/alice", "{\"mechanism\":\"PLAIN\",\"password\":\"secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().auth().clients()).containsKey("alice");
        assertThat(repository.getActiveConfig().auth().clients().get("alice").password().verify("secret"))
                .withFailMessage(() -> "Persisted client password did not verify against the submitted plaintext")
                .isTrue();
        assertThat(response.body()).doesNotContain("secret");
        assertThat(repository.updateCalls()).isEqualTo(1);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(0);
    }

    @Test
    void addsClientToSelectedGroups() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac()
                        .upsertGroup("producers", new GroupConfig(List.of(), List.of("writer")))
                        .upsertGroup("admins", new GroupConfig(List.of("bob"), List.of("admin")))));
        startServer();

        // when
        var response = send("PUT", "/auth/clients/alice", """
                {"mechanism":"PLAIN","password":"secret","groups":["producers","admins"]}
                """);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().rbac().groups().get("producers").clients())
                .containsExactly("alice");
        assertThat(repository.getActiveConfig().rbac().groups().get("admins").clients())
                .containsExactly("bob", "alice");
    }

    @Test
    void addsClientWithAppliedConsistencyWaitsForApplyMode() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/clients/alice?consistency=applied", "{\"mechanism\":\"PLAIN\",\"password\":\"secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.updateCalls()).isEqualTo(0);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(1);
    }

    @Test
    void addsClientWithoutConsistencyUsesPersistedMode() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/clients/alice", "{\"mechanism\":\"PLAIN\",\"password\":\"secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.updateCalls()).isEqualTo(1);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(0);
    }

    @Test
    void rejectsInvalidConsistencyOnClientWrite() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/clients/alice?consistency=strong", "{\"mechanism\":\"PLAIN\",\"password\":\"secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("invalid consistency 'strong'");
        assertThat(repository.updateCalls()).isEqualTo(0);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(0);
    }

    @Test
    void rejectsClientWithoutMechanism() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/clients/alice", "{\"password\":\"secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().auth().clients()).isEmpty();
    }

    @Test
    void rejectsInvalidClientBody() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/clients/alice", "not json");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().auth().clients()).isEmpty();
    }

    @Test
    void removesClientAndPersistsSnapshot() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().upsertClient("alice", client("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("DELETE", "/auth/clients/alice", null);

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.getActiveConfig().auth().clients()).isEmpty();
    }

    @Test
    void removesClientNotReferencedByAnyGroup() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().upsertClient("alice", client("PLAIN", "secret")))
                .updateRbac(GatewayConfig.empty().rbac()
                        .upsertGroup("producers", new GroupConfig(List.of("bob"), List.of("reader")))));
        startServer();

        // when
        var response = send("DELETE", "/auth/clients/alice", null);

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.getActiveConfig().auth().clients()).isEmpty();
    }

    @Test
    void removesClientReferencedByAGroup() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().upsertClient("alice", client("PLAIN", "secret")))
                .updateRbac(GatewayConfig.empty().rbac()
                        .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of("reader")))));
        startServer();

        // when
        var response = send("DELETE", "/auth/clients/alice", null);

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.getActiveConfig().auth().clients()).isEmpty();

        // and - the client is removed from the group as well
        assertThat(repository.getActiveConfig().rbac().groups().get("producers").clients())
                .withFailMessage(() -> "Client 'alice' was not removed from group 'producers'")
                .doesNotContain("alice");
    }

    @Test
    void missingClientReturnsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("DELETE", "/auth/clients/alice", null);

        // then
        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void patchChangesMechanismAndPreservesPassword() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().upsertClient("alice", client("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("PATCH", "/auth/clients/alice", "{\"mechanism\":\"SCRAM-SHA-256\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().auth().clients().get("alice"))
                .satisfies(config -> assertThat(config.password().verify("secret"))
                        .withFailMessage(() -> "Changed mechanism must preserve the existing password")
                        .isTrue());
    }

    @Test
    void patchWithAppliedConsistencyWaitsForApplyMode() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().upsertClient("alice", client("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("PATCH", "/auth/clients/alice?consistency=applied", "{\"mechanism\":\"SCRAM-SHA-256\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.updateCalls()).isEqualTo(0);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(1);
    }

    @Test
    void patchChangesPasswordAndPreservesMechanism() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().upsertClient("alice", client("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("PATCH", "/auth/clients/alice", "{\"password\":\"new-secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().auth().clients().get("alice"))
                .satisfies(config -> assertThat(config.password().verify("new-secret"))
                        .withFailMessage(() -> "Updated client password did not verify against submitted plaintext")
                        .isTrue());
    }

    @Test
    void patchReplacesClientGroups() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth()
                        .upsertClient("alice", client("PLAIN", "secret")))
                .updateRbac(GatewayConfig.empty().rbac()
                        .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of("writer")))
                        .upsertGroup("admins", new GroupConfig(List.of(), List.of("admin")))));
        startServer();

        // when
        var response = send("PATCH", "/auth/clients/alice", """
                {"groups":["admins"]}
                """);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().rbac().groups().get("producers").clients()).isEmpty();
        assertThat(repository.getActiveConfig().rbac().groups().get("admins").clients())
                .containsExactly("alice");
    }

    @Test
    void rejectsUnknownClientGroup() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/clients/alice", """
                {"mechanism":"PLAIN","password":"secret","groups":["missing"]}
                """);

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().auth().clients()).doesNotContainKey("alice");
    }

    @Test
    void patchUnknownClientReturnsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("PATCH", "/auth/clients/alice", "{\"mechanism\":\"SCRAM-SHA-256\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void patchWithNothingToPatchIsRejected() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().upsertClient("alice", client("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("PATCH", "/auth/clients/alice", "{}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("no fields to patch");
        assertThat(repository.getActiveConfig().auth().clients().get("alice"))
                .satisfies(config -> assertThat(config.password().verify("secret"))
                        .withFailMessage(() -> "Patched client password was not preserved")
                        .isTrue());
    }

    @Test
    void patchRejectsInvalidBody() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().upsertClient("alice", client("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("PATCH", "/auth/clients/alice", "not json");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).startsWith("{\"error\":\"invalid client body");
    }
}
