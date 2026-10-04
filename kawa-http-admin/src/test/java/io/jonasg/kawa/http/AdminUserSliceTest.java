package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminUserConfig;
import io.jonasg.kawa.config.GatewayConfig;
import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for the `/admin/users` admin surface.
class AdminUserSliceTest extends AdminHttpSliceTestBase {

    @Test
    void listsAdminUsersSortedWithoutHashes() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertAdminUser("bob", AdminUserConfig.fromPlaintext("bob-secret"))
                .upsertAdminUser("alice", AdminUserConfig.fromPlaintext("alice-secret")));
        startServer();

        // when
        var response = send("GET", "/admin/users");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("""
                [{"username":"alice"},{"username":"bob"}]
                """);
    }

    @Test
    void createsAdminUserWithHashedPassword() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/admin/users/alice", """
                {"password":"alice-secret"}
                """);

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("""
                {"username":"alice"}
                """);
        AdminUserConfig stored = repository.getActiveConfig().adminUsers().get("alice");
        assertThat(stored.password().verify("alice-secret"))
                .withFailMessage(() -> "the stored admin user does not verify with its password")
                .isTrue();
    }

    @Test
    void rejectsMissingPassword() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/admin/users/alice", "{}");

        // then
        assertThat(response).hasStatusCode(400);
        assertThatJson(response.body()).isEqualTo("""
                {"error":"admin user 'alice': password is required"}
                """);
    }

    @Test
    void rejectsManagingTheBootstrapUser() throws Exception {
        // given
        enableOAuth();
        startServer();
        String token = login("admin", "s3cret");

        // when
        var response = send("PUT", "/admin/users/admin", """
                {"password":"new-secret"}
                """, bearer(token));

        // then
        assertThat(response).hasStatusCode(400);
        assertThatJson(response.body()).isEqualTo("""
                {"error":"admin user 'admin' is the bootstrap user and is managed in the gateway config file"}
                """);
    }

    @Test
    void deletesAdminUser() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .upsertAdminUser("alice", AdminUserConfig.fromPlaintext("alice-secret")));
        startServer();

        // when
        var response = send("DELETE", "/admin/users/alice");

        // then
        assertThat(response).hasStatusCode(204);
        assertThat(repository.getActiveConfig().adminUsers())
                .withFailMessage(() -> "admin user 'alice' was not removed")
                .doesNotContainKey("alice");
    }

    @Test
    void deletingUnknownAdminUserIsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("DELETE", "/admin/users/ghost");

        // then
        assertThat(response).hasStatusCode(404);
    }
}
