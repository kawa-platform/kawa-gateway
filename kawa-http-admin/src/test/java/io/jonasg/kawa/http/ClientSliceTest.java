package io.jonasg.kawa.http;

import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.ManagedBy;
import io.jonasg.kawa.config.Mechanism;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for the `/auth/clients` admin surface: real HTTP requests through a booted
/// [AdminHttpServer], asserting the JSON wire format the admin UI consumes.
class ClientSliceTest extends AdminHttpSliceTestBase {

    private static final ManagedBy OPERATOR = new ManagedBy("kawa-operator");

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
                [{"username":"alice","mechanism":"PLAIN","managedBy":null}]
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
                  {"username":"alice","mechanism":"PLAIN","managedBy":null},
                  {"username":"bob","mechanism":"PLAIN","managedBy":null}
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
    }

    @Test
    void addsClientWithoutConsistencyUsesPersistedMode() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/clients/alice", "{\"mechanism\":\"PLAIN\",\"password\":\"secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
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

    @Test
    void listsClientsWithExactlyUsernameAndMechanism() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().upsertClient("alice", client("PLAIN", "secret"))));
        startServer();

        // when
        var clientsResp = send("GET", "/auth/clients", null);

        // then
        assertThat(clientsResp.statusCode()).isEqualTo(200);
        assertThat(clientsResp).containsExactlyTopLevelPropertyNames("username", "mechanism", "managedBy");
    }

    @Test
    void putResponseOmitsPassword() throws Exception {
        // given
        startServer();

        // when
        var putResp = send("PUT", "/auth/clients/alice", "{\"mechanism\":\"PLAIN\",\"password\":\"secret\"}");

        // then
        assertThat(putResp.statusCode()).isEqualTo(200);
        assertThat(putResp).containsExactlyTopLevelPropertyNames("username", "mechanism", "managedBy");
    }

    @Test
    void patchResponseOmitsPassword() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().upsertClient("alice", client("PLAIN", "secret"))));
        startServer();

        // when
        var patchResp = send("PATCH", "/auth/clients/alice", "{\"mechanism\":\"SCRAM-SHA-256\"}");

        // then
        assertThat(patchResp.statusCode()).isEqualTo(200);
        assertThat(patchResp).containsExactlyTopLevelPropertyNames("username", "mechanism", "managedBy");
    }

    @Test
    void anOperatorWriteMarksTheClientAsManagedByTheOperator() throws Exception {
        // given
        auth = operatorAuth();
        startServer();

        // when
        var putResp = send("PUT", "/auth/clients/alice", "{\"mechanism\":\"PLAIN\",\"password\":\"secret\"}", bearer(operatorToken()));

        // then
        assertThat(putResp).hasStatusCode(200);
        assertThatJson(putResp.body()).isEqualTo("""
                {"username": "alice", "mechanism": "PLAIN", "managedBy": {"principal": "kawa-operator"}}
                """);
        assertThat(repository.getActiveConfig().auth().clients().get("alice").managedBy())
                .withFailMessage(() -> "client 'alice' was not stamped as managed by the operator")
                .isEqualTo(OPERATOR);
    }

    @Test
    void listsTheOwnerOfAManagedClient() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().upsertClient("alice", client("PLAIN", "secret").withManagedBy(OPERATOR))));
        startServer();

        // when
        var clientsResp = send("GET", "/auth/clients", null);

        // then
        assertThat(clientsResp).hasStatusCode(200);
        assertThatJson(clientsResp.body()).isEqualTo("""
                [{"username": "alice", "mechanism": "PLAIN", "managedBy": {"principal": "kawa-operator"}}]
                """);
    }

    @Test
    void refusesChangesToAManagedClientByAnyoneButItsOwner() throws Exception {
        // given
        auth = operatorAuth();
        var config = GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().upsertClient("alice", client("PLAIN", "secret").withManagedBy(OPERATOR)));
        repository = new FakeGatewayConfigRepository(config);
        startServer();
        var studio = bearer(login("admin@kawa", "s3cret"));

        // when
        var putResp = send("PUT", "/auth/clients/alice", "{\"mechanism\":\"PLAIN\",\"password\":\"other\"}", studio);
        var passwordResetResp = send("PATCH", "/auth/clients/alice", "{\"password\":\"other\"}", studio);
        var deleteResp = send("DELETE", "/auth/clients/alice", null, studio);

        // then
        for (var response : List.of(putResp, passwordResetResp, deleteResp)) {
            assertThat(response).hasStatusCode(409);
            assertThatJson(response.body()).isEqualTo("""
                    {"error": "client 'alice' is managed by kawa-operator; change it through Kubernetes"}
                    """);
        }
        assertThat(repository.getActiveConfig().auth().clients())
                .withFailMessage(() -> "a refused write changed managed client 'alice'")
                .isEqualTo(config.auth().clients());
    }

    @Test
    void refusesClientWritesThatChangeAManagedGroupsMembership() throws Exception {
        // given: bob is in the managed group 'producers', carol is in no group
        auth = operatorAuth();
        var base = GatewayConfig.empty().updateAuth(GatewayConfig.empty().auth()
                .upsertClient("bob", client("PLAIN", "secret"))
                .upsertClient("carol", client("PLAIN", "secret")));
        var config = base.updateRbac(base.rbac().upsertGroup("producers", new GroupConfig(List.of("bob"), List.of(), OPERATOR)));
        repository = new FakeGatewayConfigRepository(config);
        startServer();
        var studio = bearer(login("admin@kawa", "s3cret"));

        // when
        var joinResp = send("PUT", "/auth/clients/carol", "{\"mechanism\":\"PLAIN\",\"password\":\"secret\",\"groups\":[\"producers\"]}", studio);
        var leaveResp = send("PATCH", "/auth/clients/bob", "{\"groups\":[]}", studio);
        var deleteResp = send("DELETE", "/auth/clients/bob", null, studio);

        // then
        for (var response : List.of(joinResp, leaveResp, deleteResp)) {
            assertThat(response).hasStatusCode(409);
            assertThatJson(response.body()).isEqualTo("""
                    {"error": "group 'producers' is managed by kawa-operator; change it through Kubernetes"}
                    """);
        }
        assertThat(repository.getActiveConfig().rbac().groups().get("producers"))
                .withFailMessage(() -> "a refused client write changed managed group 'producers'")
                .isEqualTo(new GroupConfig(List.of("bob"), List.of(), OPERATOR));
        assertThat(repository.getActiveConfig().auth().clients())
                .withFailMessage(() -> "a refused client write changed the clients")
                .isEqualTo(config.auth().clients());
    }

    @Test
    void aStudioClientWriteThatLeavesManagedGroupsAloneIsAllowedAndUnmanaged() throws Exception {
        // given
        auth = operatorAuth();
        var base = GatewayConfig.empty().updateAuth(GatewayConfig.empty().auth().upsertClient("bob", client("PLAIN", "secret")));
        repository = new FakeGatewayConfigRepository(base.updateRbac(base.rbac()
                .upsertGroup("producers", new GroupConfig(List.of("bob"), List.of(), OPERATOR))));
        startServer();

        // when
        var passwordResetResp = send("PATCH", "/auth/clients/bob", "{\"password\":\"other\"}", bearer(login("admin@kawa", "s3cret")));

        // then
        assertThat(passwordResetResp).hasStatusCode(200);
        assertThatJson(passwordResetResp.body()).inPath("managedBy").isNull();
        assertThat(repository.getActiveConfig().auth().clients().get("bob").password().verify("other"))
                .withFailMessage(() -> "the password reset of unmanaged client 'bob' was not applied")
                .isTrue();
        assertThat(repository.getActiveConfig().rbac().groups().get("producers").clients())
                .withFailMessage(() -> "client 'bob' was dropped from managed group 'producers'")
                .containsExactly("bob");
    }

    @Test
    void onlyTheOwnerReleasesAManagedClient() throws Exception {
        // given
        auth = operatorAuth();
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty().updateAuth(GatewayConfig.empty().auth()
                .upsertClient("alice", client("PLAIN", "secret").withManagedBy(OPERATOR))
                .upsertClient("bob", client("PLAIN", "secret"))));
        startServer();

        // when
        var studioReleaseResp = send("DELETE", "/auth/clients/alice/managed-by", null, bearer(login("admin@kawa", "s3cret")));
        var operatorReleaseResp = send("DELETE", "/auth/clients/alice/managed-by", null, bearer(operatorToken()));
        var unmanagedReleaseResp = send("DELETE", "/auth/clients/bob/managed-by", null, bearer(operatorToken()));
        var missingReleaseResp = send("DELETE", "/auth/clients/missing/managed-by", null, bearer(operatorToken()));

        // then
        assertThat(studioReleaseResp).hasStatusCode(409);
        assertThat(operatorReleaseResp).hasStatusCode(204);
        assertThat(unmanagedReleaseResp).hasStatusCode(204);
        assertThat(missingReleaseResp).hasStatusCode(404);
        assertThat(repository.getActiveConfig().auth().clients().get("alice").managedBy())
                .withFailMessage(() -> "the operator's release did not clear the owner of client 'alice'")
                .isNull();
    }
}
