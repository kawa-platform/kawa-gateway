package io.jonasg.kawa.http;

import io.jonasg.kawa.config.DynamicConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.ManagedBy;
import io.jonasg.kawa.config.RoleConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for the `/rbac/roles` admin surface: real HTTP requests through a booted
/// [AdminHttpServer], asserting the JSON wire format the admin UI consumes.
class RoleSliceTest extends AdminHttpSliceTestBase {

    private static final ManagedBy OPERATOR = new ManagedBy("kawa-operator");

    @Test
    void listsConfiguredRoles() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac().upsertRole("reader", new RoleConfig(null))));
        startServer();

        // when
        var response = send("GET", "/rbac/roles", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                [{"name":"reader","acls":[],"managedBy":null}]
                """);
    }

    @Test
    void listsRolesSortedByName() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac()
                        .upsertRole("writer", new RoleConfig(null))
                        .upsertRole("reader", new RoleConfig(null))));
        startServer();

        // when
        var response = send("GET", "/rbac/roles", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                [
                  {"name":"reader","acls":[],"managedBy":null},
                  {"name":"writer","acls":[],"managedBy":null}
                ]
                """);
    }

    @Test
    void listsRolesEmptyWhenNoSnapshotApplied() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(null);
        startServer();

        // when
        var response = send("GET", "/rbac/roles", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("[]");
    }

    @Test
    void addsRoleAndPersistsSnapshot() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/rbac/roles/reader",
                "{\"acls\":[{\"resource\":{\"type\":\"TOPIC\",\"pattern\":\"orders\"},\"operation\":\"READ\"}]}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThatJson(response.body()).isEqualTo("""
                {
                  "acls": [{
                    "resource": {"type": "TOPIC", "pattern": "orders", "patternType": "LITERAL"},
                    "operation": "READ",
                    "permission": "ALLOW"
                  }],
                  "managedBy": null
                }
                """);
        assertThat(repository.getActiveConfig().rbac().roles()).containsKey("reader");
        assertThat(repository.getActiveConfig().rbac().roles().get("reader").acls()).hasSize(1);
    }

    @Test
    void rejectsInvalidRoleBody() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/rbac/roles/reader", "not json");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().rbac().roles()).isEmpty();
    }

    @Test
    void removesRoleAndPersistsSnapshot() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac().upsertRole("reader", new RoleConfig(null))));
        startServer();

        // when
        var response = send("DELETE", "/rbac/roles/reader", null);

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.getActiveConfig().rbac().roles()).isEmpty();
    }

    @Test
    void removesRoleFromGroupsThatReferenceIt() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac()
                        .upsertRole("reader", new RoleConfig(null))
                        .upsertGroup("producers", new GroupConfig(List.of("alice"), List.of("reader")))));
        startServer();

        // when
        var response = send("DELETE", "/rbac/roles/reader", null);

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.getActiveConfig().rbac().roles()).isEmpty();
        assertThat(repository.getActiveConfig().rbac().groups().get("producers").roles()).isEmpty();
        assertThat(repository.getActiveConfig().rbac().groups().get("producers").clients())
                .containsExactly("alice");
    }

    @Test
    void missingRoleReturnsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("DELETE", "/rbac/roles/reader", null);

        // then
        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void listsRolesWithExactlyNameAndAcls() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac().upsertRole("reader", new RoleConfig(null))));
        startServer();

        // when
        var rolesResp = send("GET", "/rbac/roles", null);

        // then
        assertThat(rolesResp.statusCode()).isEqualTo(200);
        assertThat(rolesResp)
                .containsExactlyTopLevelPropertyNames("name", "acls", "managedBy");
    }

    @Test
    void putResponseOmitsName() throws Exception {
        // given
        startServer();

        // when
        var putResp = send("PUT", "/rbac/roles/reader", "{\"acls\":[]}");

        // then
        assertThat(putResp.statusCode()).isEqualTo(200);
        assertThat(putResp).containsExactlyTopLevelPropertyNames("acls", "managedBy");
    }

    @Test
    void anOperatorWriteMarksTheRoleAsManagedByTheOperator() throws Exception {
        // given
        auth = operatorAuth();
        startServer();

        // when
        var putResp = send("PUT", "/rbac/roles/reader", "{\"acls\":[]}", bearer(operatorToken()));

        // then
        assertThat(putResp).hasStatusCode(200);
        assertThatJson(putResp.body()).isEqualTo("""
                {"acls": [], "managedBy": {"principal": "kawa-operator"}}
                """);
        assertThat(repository.getActiveConfig().rbac().roles().get("reader"))
                .withFailMessage(() -> "role 'reader' was not stamped as managed by the operator")
                .isEqualTo(new RoleConfig(List.of(), OPERATOR));
    }

    @Test
    void listsTheOwnerOfAManagedRole() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac().upsertRole("reader", new RoleConfig(List.of(), OPERATOR))));
        startServer();

        // when
        var rolesResp = send("GET", "/rbac/roles", null);

        // then
        assertThat(rolesResp).hasStatusCode(200);
        assertThatJson(rolesResp.body()).isEqualTo("""
                [{"name": "reader", "acls": [], "managedBy": {"principal": "kawa-operator"}}]
                """);
    }

    @Test
    void refusesChangesToAManagedRoleByAnyoneButItsOwner() throws Exception {
        // given
        auth = operatorAuth();
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac().upsertRole("reader", new RoleConfig(List.of(), OPERATOR))));
        startServer();
        var studio = bearer(login("admin@kawa", "s3cret"));

        // when
        var putResp = send("PUT", "/rbac/roles/reader", "{\"acls\":[]}", studio);
        var deleteResp = send("DELETE", "/rbac/roles/reader", null, studio);

        // then
        for (var response : List.of(putResp, deleteResp)) {
            assertThat(response).hasStatusCode(409);
            assertThatJson(response.body()).isEqualTo("""
                    {"error": "role 'reader' is managed by kawa-operator; change it through Kubernetes"}
                    """);
        }
        assertThat(repository.getActiveConfig().rbac().roles())
                .withFailMessage(() -> "a refused write changed managed role 'reader': " + repository.getActiveConfig().rbac().roles())
                .containsExactly(Map.entry("reader", new RoleConfig(List.of(), OPERATOR)));
    }

    @Test
    void refusesDeletingARoleAManagedGroupReferences() throws Exception {
        // given
        auth = operatorAuth();
        var rbac = DynamicConfig.empty().rbac()
                .upsertRole("reader", new RoleConfig(List.of()))
                .upsertGroup("producers", new GroupConfig(List.of(), List.of("reader"), OPERATOR));
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().updateRbac(rbac));
        startServer();

        // when
        var deleteResp = send("DELETE", "/rbac/roles/reader", null, bearer(login("admin@kawa", "s3cret")));

        // then
        assertThat(deleteResp).hasStatusCode(409);
        assertThatJson(deleteResp.body()).isEqualTo("""
                {"error": "group 'producers' is managed by kawa-operator; change it through Kubernetes"}
                """);
        assertThat(repository.getActiveConfig().rbac())
                .withFailMessage(() -> "a refused role delete changed the rbac section: " + repository.getActiveConfig().rbac())
                .isEqualTo(rbac);
    }

    @Test
    void theOwnerRewritesItsManagedRoleAndOthersKeepEditingUnmanagedOnes() throws Exception {
        // given
        auth = operatorAuth();
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().updateRbac(DynamicConfig.empty().rbac()
                .upsertRole("reader", new RoleConfig(List.of(), OPERATOR))
                .upsertRole("writer", new RoleConfig(List.of()))));
        startServer();
        var acls = """
                {"acls": [{"resource": {"type": "TOPIC", "pattern": "orders"}, "operation": "READ"}]}
                """;

        // when
        var operatorPutResp = send("PUT", "/rbac/roles/reader", acls, bearer(operatorToken()));
        var studioPutResp = send("PUT", "/rbac/roles/writer", acls, bearer(login("admin@kawa", "s3cret")));

        // then
        assertThat(operatorPutResp).hasStatusCode(200);
        assertThat(studioPutResp).hasStatusCode(200);
        assertThatJson(studioPutResp.body()).inPath("managedBy").isNull();
        var roles = repository.getActiveConfig().rbac().roles();
        assertThat(roles.get("reader").managedBy())
                .withFailMessage(() -> "the operator's rewrite of role 'reader' dropped its owner")
                .isEqualTo(OPERATOR);
        assertThat(roles.get("writer").managedBy())
                .withFailMessage(() -> "a studio write did not leave role 'writer' unmanaged")
                .isNull();
        assertThat(roles.get("writer").acls())
                .withFailMessage(() -> "the studio write to unmanaged role 'writer' was not applied")
                .hasSize(1);
    }

    @Test
    void onlyTheOwnerReleasesAManagedRole() throws Exception {
        // given
        auth = operatorAuth();
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .updateRbac(DynamicConfig.empty().rbac().upsertRole("reader", new RoleConfig(List.of(), OPERATOR))));
        startServer();

        // when
        var studioReleaseResp = send("DELETE", "/rbac/roles/reader/managed-by", null, bearer(login("admin@kawa", "s3cret")));
        var operatorReleaseResp = send("DELETE", "/rbac/roles/reader/managed-by", null, bearer(operatorToken()));
        var missingReleaseResp = send("DELETE", "/rbac/roles/missing/managed-by", null, bearer(operatorToken()));

        // then
        assertThat(studioReleaseResp).hasStatusCode(409);
        assertThat(operatorReleaseResp).hasStatusCode(204);
        assertThat(missingReleaseResp).hasStatusCode(404);
        assertThat(repository.getActiveConfig().rbac().roles().get("reader"))
                .withFailMessage(() -> "the operator's release did not clear the owner of role 'reader'")
                .isEqualTo(new RoleConfig(List.of()));
    }
}
