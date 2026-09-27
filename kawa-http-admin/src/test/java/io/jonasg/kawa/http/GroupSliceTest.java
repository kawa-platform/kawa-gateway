package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GroupConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static io.jonasg.kawa.http.KawaAssertions.assertThat;

/// Slice tests for the `/rbac/groups` admin surface: real HTTP requests through a booted
/// [AdminHttpServer], asserting the JSON wire format the admin UI consumes.
class GroupSliceTest extends AdminHttpSliceTestBase {

    @Test
    void listsConfiguredGroups() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac().upsertGroup("producers", new GroupConfig(null, null))));
        startServer();

        // when
        var response = send("GET", "/rbac/groups", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                [{"name":"producers","clients":[],"roles":[]}]
                """);
    }

    @Test
    void listsGroupsSortedByName() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac()
                        .upsertGroup("writers", new GroupConfig(null, null))
                        .upsertGroup("producers", new GroupConfig(null, null))));
        startServer();

        // when
        var response = send("GET", "/rbac/groups", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                [
                  {"name":"producers","clients":[],"roles":[]},
                  {"name":"writers","clients":[],"roles":[]}
                ]
                """);
    }

    @Test
    void listsGroupsEmptyWhenNoSnapshotApplied() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(null);
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
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac().upsertGroup("producers", new GroupConfig(null, null))));
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
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac()
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
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac()
                        .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of("reader")))));
        startServer();

        // when
        var response = send("PATCH", "/rbac/groups/producers", "{\"name\":\"publishers\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                {"name":"publishers","clients":["alice"],"roles":["reader"]}
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
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac()
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
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac()
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
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac()
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
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac()
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
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac().upsertGroup("producers", new GroupConfig(null, null))));
        startServer();

        // when
        var groupsResp = send("GET", "/rbac/groups", null);

        // then
        assertThat(groupsResp.statusCode()).isEqualTo(200);
        assertThat(groupsResp).containsExactlyTopLevelPropertyNames("name", "clients", "roles");
    }

    @Test
    void putResponseOmitsName() throws Exception {
        // given
        startServer();

        // when
        var putResp = send("PUT", "/rbac/groups/producers", "{\"clients\":[],\"roles\":[]}");

        // then
        assertThat(putResp.statusCode()).isEqualTo(200);
        assertThat(putResp).containsExactlyTopLevelPropertyNames("clients", "roles");
    }

    @Test
    void patchResponseIncludesName() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac()
                        .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of("reader")))));
        startServer();

        // when
        var patchResp = send("PATCH", "/rbac/groups/producers", "{\"name\":\"publishers\"}");

        // then
        assertThat(patchResp.statusCode()).isEqualTo(200);
        assertThat(patchResp).containsExactlyTopLevelPropertyNames("name", "clients", "roles");
    }
}
