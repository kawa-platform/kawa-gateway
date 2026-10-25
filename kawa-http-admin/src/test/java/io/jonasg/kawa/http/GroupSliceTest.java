package io.jonasg.kawa.http;

import io.jonasg.kawa.config.DynamicConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.ManagedBy;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for the `/rbac/groups` admin surface: real HTTP requests through a booted
/// [AdminHttpServer], asserting the JSON wire format the admin UI consumes.
class GroupSliceTest extends AdminHttpSliceTestBase {

    private static final ManagedBy OPERATOR = new ManagedBy("kawa-operator");

    @Test
    void listsConfiguredGroups() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac().upsertGroup("producers", new GroupConfig(null, null))));
        startServer();

        // when
        var response = send("GET", "/rbac/groups", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                [{"name":"producers","clients":[],"roles":[],"managedBy":null}]
                """);
    }

    @Test
    void listsGroupsSortedByName() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac()
                        .upsertGroup("writers", new GroupConfig(null, null))
                        .upsertGroup("producers", new GroupConfig(null, null))));
        startServer();

        // when
        var response = send("GET", "/rbac/groups", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                [
                  {"name":"producers","clients":[],"roles":[],"managedBy":null},
                  {"name":"writers","clients":[],"roles":[],"managedBy":null}
                ]
                """);
    }

    @Test
    void listsGroupsEmptyWhenNoSnapshotApplied() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(null);
        startServer();

        // when
        var response = send("GET", "/rbac/groups", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("[]");
    }

    @Test
    void addsGroupAndPersistsSnapshot() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/rbac/groups/producers", "{\"clients\":[\"alice\"],\"roles\":[\"reader\"]}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().rbac().groups()).containsKey("producers");
        assertThat(repository.getActiveConfig().rbac().groups().get("producers").clients())
                .containsExactly("alice");
    }

    @Test
    void rejectsInvalidGroupBody() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/rbac/groups/producers", "not json");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().rbac().groups()).isEmpty();
    }

    @Test
    void removesGroupAndPersistsSnapshot() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac().upsertGroup("producers", new GroupConfig(null, null))));
        startServer();

        // when
        var response = send("DELETE", "/rbac/groups/producers", null);

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.getActiveConfig().rbac().groups()).isEmpty();
    }

    @Test
    void rejectsRemovalWhileGroupHasClients() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac()
                        .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of("reader")))));
        startServer();

        // when
        var response = send("DELETE", "/rbac/groups/producers", null);

        // then
        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(response.body()).contains("still has clients", "alice");
        assertThat(repository.getActiveConfig().rbac().groups()).containsKey("producers");
    }

    @Test
    void missingGroupReturnsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("DELETE", "/rbac/groups/producers", null);

        // then
        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void renamesGroupAndPersistsSnapshot() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac()
                        .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of("reader")))));
        startServer();

        // when
        var response = send("PATCH", "/rbac/groups/producers", "{\"name\":\"publishers\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                {"name":"publishers","clients":["alice"],"roles":["reader"],"managedBy":null}
                """);
        assertThat(repository.getActiveConfig().rbac().groups()).doesNotContainKey("producers");
        assertThat(repository.getActiveConfig().rbac().groups().get("publishers"))
                .isEqualTo(new GroupConfig(List.of("alice"), List.of("reader")));
    }

    @Test
    void renameMissingGroupReturnsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("PATCH", "/rbac/groups/producers", "{\"name\":\"publishers\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void renameToExistingGroupIsRejected() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac()
                        .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of("reader")))
                        .upsertGroup("publishers", new GroupConfig(List.of("bob"), List.of()))));
        startServer();

        // when
        var response = send("PATCH", "/rbac/groups/producers", "{\"name\":\"publishers\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(repository.getActiveConfig().rbac().groups()).containsKeys("producers", "publishers");
    }

    @Test
    void renameToBlankNameIsRejected() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac()
                        .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of()))));
        startServer();

        // when
        var response = send("PATCH", "/rbac/groups/producers", "{\"name\":\"\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().rbac().groups()).containsKey("producers");
    }

    @Test
    void renameToSameNameIsRejected() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac()
                        .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of()))));
        startServer();

        // when
        var response = send("PATCH", "/rbac/groups/producers", "{\"name\":\"producers\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
    }

    @Test
    void renameRejectsInvalidBody() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac()
                        .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of()))));
        startServer();

        // when
        var response = send("PATCH", "/rbac/groups/producers", "not json");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).startsWith("{\"error\":\"invalid group body");
    }

    @Test
    void listsGroupsWithExactlyNameClientsAndRoles() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac().upsertGroup("producers", new GroupConfig(null, null))));
        startServer();

        // when
        var groupsResp = send("GET", "/rbac/groups", null);

        // then
        assertThat(groupsResp.statusCode()).isEqualTo(200);
        assertThat(groupsResp).containsExactlyTopLevelPropertyNames("name", "clients", "roles", "managedBy");
    }

    @Test
    void putResponseOmitsName() throws Exception {
        // given
        startServer();

        // when
        var putResp = send("PUT", "/rbac/groups/producers", "{\"clients\":[],\"roles\":[]}");

        // then
        assertThat(putResp.statusCode()).isEqualTo(200);
        assertThat(putResp).containsExactlyTopLevelPropertyNames("clients", "roles", "managedBy");
    }

    @Test
    void patchResponseIncludesName() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac()
                        .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of("reader")))));
        startServer();

        // when
        var patchResp = send("PATCH", "/rbac/groups/producers", "{\"name\":\"publishers\"}");

        // then
        assertThat(patchResp.statusCode()).isEqualTo(200);
        assertThat(patchResp).containsExactlyTopLevelPropertyNames("name", "clients", "roles", "managedBy");
    }

    @Test
    void anOperatorWriteMarksTheGroupAsManagedByTheOperator() throws Exception {
        // given
        auth = operatorAuth();
        startServer();

        // when
        var putResp = send("PUT", "/rbac/groups/producers", "{\"clients\":[],\"roles\":[]}", bearer(operatorToken()));

        // then
        assertThat(putResp).hasStatusCode(200);
        assertThatJson(putResp.body()).isEqualTo("""
                {"clients": [], "roles": [], "managedBy": {"principal": "kawa-operator"}}
                """);
        assertThat(repository.getActiveConfig().rbac().groups().get("producers"))
                .withFailMessage(() -> "group 'producers' was not stamped as managed by the operator")
                .isEqualTo(new GroupConfig(List.of(), List.of(), OPERATOR));
    }

    @Test
    void listsTheOwnerOfAManagedGroup() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().updateRbac(DynamicConfig.empty().rbac()
                .upsertGroup("producers", new GroupConfig(List.of(), List.of(), OPERATOR))));
        startServer();

        // when
        var groupsResp = send("GET", "/rbac/groups", null);

        // then
        assertThat(groupsResp).hasStatusCode(200);
        assertThatJson(groupsResp.body()).isEqualTo("""
                [{"name": "producers", "clients": [], "roles": [], "managedBy": {"principal": "kawa-operator"}}]
                """);
    }

    @Test
    void refusesChangesToAManagedGroupByAnyoneButItsOwner() throws Exception {
        // given
        auth = operatorAuth();
        var managed = new GroupConfig(List.of(), List.of(), OPERATOR);
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().updateRbac(DynamicConfig.empty().rbac()
                .upsertGroup("producers", managed)));
        startServer();
        var studio = bearer(login("admin@kawa", "s3cret"));

        // when
        var putResp = send("PUT", "/rbac/groups/producers", "{\"clients\":[],\"roles\":[]}", studio);
        var patchResp = send("PATCH", "/rbac/groups/producers", "{\"name\":\"publishers\"}", studio);
        var deleteResp = send("DELETE", "/rbac/groups/producers", null, studio);

        // then
        for (var response : List.of(putResp, patchResp, deleteResp)) {
            assertThat(response).hasStatusCode(409);
            assertThatJson(response.body()).isEqualTo("""
                    {"error": "group 'producers' is managed by kawa-operator; change it through Kubernetes"}
                    """);
        }
        assertThat(repository.getActiveConfig().rbac().groups())
                .withFailMessage(() -> "a refused write changed managed group 'producers': " + repository.getActiveConfig().rbac().groups())
                .containsExactly(Map.entry("producers", managed));
    }

    @Test
    void theOwnerRenamesItsManagedGroupAndKeepsOwningIt() throws Exception {
        // given
        auth = operatorAuth();
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().updateRbac(DynamicConfig.empty().rbac()
                .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of(), OPERATOR))));
        startServer();

        // when
        var patchResp = send("PATCH", "/rbac/groups/producers", "{\"name\":\"publishers\"}", bearer(operatorToken()));

        // then
        assertThat(patchResp).hasStatusCode(200);
        assertThatJson(patchResp.body()).isEqualTo("""
                {"name": "publishers", "clients": ["alice"], "roles": [], "managedBy": {"principal": "kawa-operator"}}
                """);
        assertThat(repository.getActiveConfig().rbac().groups())
                .withFailMessage(() -> "the operator's rename did not move managed group 'producers': " + repository.getActiveConfig().rbac().groups())
                .containsExactly(Map.entry("publishers", new GroupConfig(List.of("alice"), List.of(), OPERATOR)));
    }

    @Test
    void aStudioWriteLeavesAnUnmanagedGroupUnmanaged() throws Exception {
        // given
        auth = operatorAuth();
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().updateRbac(DynamicConfig.empty().rbac()
                .upsertGroup("producers", new GroupConfig(List.of(), List.of()))));
        startServer();

        // when
        var putResp = send("PUT", "/rbac/groups/producers", "{\"clients\":[\"alice\"],\"roles\":[]}", bearer(login("admin@kawa", "s3cret")));

        // then
        assertThat(putResp).hasStatusCode(200);
        assertThat(repository.getActiveConfig().rbac().groups().get("producers"))
                .withFailMessage(() -> "a studio write did not leave group 'producers' unmanaged")
                .isEqualTo(new GroupConfig(List.of("alice"), List.of()));
    }

    @Test
    void onlyTheOwnerReleasesAManagedGroup() throws Exception {
        // given
        auth = operatorAuth();
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().updateRbac(DynamicConfig.empty().rbac()
                .upsertGroup("producers", new GroupConfig(List.of(), List.of(), OPERATOR))
                .upsertGroup("consumers", new GroupConfig(List.of(), List.of()))));
        startServer();

        // when
        var studioReleaseResp = send("DELETE", "/rbac/groups/producers/managed-by", null, bearer(login("admin@kawa", "s3cret")));
        var operatorReleaseResp = send("DELETE", "/rbac/groups/producers/managed-by", null, bearer(operatorToken()));
        var unmanagedReleaseResp = send("DELETE", "/rbac/groups/consumers/managed-by", null, bearer(operatorToken()));
        var missingReleaseResp = send("DELETE", "/rbac/groups/missing/managed-by", null, bearer(operatorToken()));

        // then
        assertThat(studioReleaseResp).hasStatusCode(409);
        assertThat(operatorReleaseResp).hasStatusCode(204);
        assertThat(unmanagedReleaseResp).hasStatusCode(204);
        assertThat(missingReleaseResp).hasStatusCode(404);
        assertThat(repository.getActiveConfig().rbac().groups().get("producers"))
                .withFailMessage(() -> "the operator's release did not clear the owner of group 'producers'")
                .isEqualTo(new GroupConfig(List.of(), List.of()));
    }
}
