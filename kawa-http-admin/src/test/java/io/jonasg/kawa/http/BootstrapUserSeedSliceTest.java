package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminUserConfig;
import io.jonasg.kawa.config.DynamicConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.jonasg.kawa.http.AdminUserConfigMother.ALICE_ID;
import static io.jonasg.kawa.http.AdminUserConfigMother.CREATED_AT;
import static io.jonasg.kawa.http.AdminUserConfigMother.adminUser;
import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Slice tests for seeding `admin.auth.bootstrapUser` when the [AdminHttpServer] starts: it becomes a
/// stored admin user on a gateway without any, is ignored once admin users exist, and is restored by
/// the `KAWA_ADMIN_RESET` switch. Seeding has no handler of its own, so it is driven by booting the
/// server, like [ServerSliceTest].
class BootstrapUserSeedSliceTest extends AdminHttpSliceTestBase {

    private static final String BOOTSTRAP_ID = BootstrapUserSeeder.idOf("admin@kawa");

    @Test
    void seedsTheBootstrapUserAsAStoredAdminUserOnAGatewayWithoutAny() throws Exception {
        // given
        enableOAuth();
        startServer();

        // when
        var response = send("GET", "/admin/users", null, bearer(login("admin@kawa", "s3cret")));

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).whenIgnoringPaths("[0].createdAt").isEqualTo("""
                [{"id":"%s","email":"admin@kawa","displayName":null,"enabled":true}]
                """.formatted(BOOTSTRAP_ID));
        assertThat(seeded().password().encoded())
                .withFailMessage(() -> "the bootstrap password was stored in plaintext")
                .doesNotContain("s3cret");
    }

    @Test
    void ignoresTheBootstrapUserWhenAdminUsersExist() throws Exception {
        // given
        enableOAuth();
        DynamicConfig existing = DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withPassword("alice-secret").build());
        repository = new FakeDynamicConfigRepository(existing);

        // when
        startServer();

        // then
        assertThat(repository.getActiveConfig())
                .withFailMessage(() -> "a start with admin users present changed them: " + repository.getActiveConfig().adminUsers())
                .isSameAs(existing);
        assertThat(signIn("admin@kawa", "s3cret")).hasStatusCode(400);
    }

    @Test
    void keepsADeletedBootstrapUserDeletedAcrossARestart() throws Exception {
        // given: the seeded admin creates alice, who deletes the bootstrap user
        enableOAuth();
        startServer();
        send("POST", "/admin/users", """
                {"email":"alice@mycom.com","password":"alice-secret"}
                """, bearer(login("admin@kawa", "s3cret")));
        var deleted = send("DELETE", "/admin/users/" + BOOTSTRAP_ID, null, bearer(login("alice@mycom.com", "alice-secret")));
        assertThat(deleted).hasStatusCode(204);
        server().stop();

        // when
        startServer();

        // then
        assertThat(repository.getActiveConfig().adminUsers())
                .withFailMessage(() -> "the deleted bootstrap user came back after a restart")
                .doesNotContainKey(BOOTSTRAP_ID)
                .hasSize(1);
    }

    @Test
    void instancesStartingOnAnEmptyTopicSeedTheSameAdminUser() throws Exception {
        // given: two gateway instances, each still seeing the empty config topic
        enableOAuth();
        var first = new FakeDynamicConfigRepository(DynamicConfig.empty());
        var second = new FakeDynamicConfigRepository(DynamicConfig.empty());

        // when
        repository = first;
        startServer();
        server().stop();
        repository = second;
        startServer();

        // then: both write the same id, so the snapshots converge on one admin user
        assertThat(first.getActiveConfig().adminUsers().keySet())
                .withFailMessage(() -> "instances seeded the bootstrap user under different ids: "
                        + first.getActiveConfig().adminUsers().keySet() + " and " + second.getActiveConfig().adminUsers().keySet())
                .isEqualTo(second.getActiveConfig().adminUsers().keySet())
                .containsExactly(BOOTSTRAP_ID);
    }

    @Test
    void resetReEnablesTheBootstrapUserAndResetsItsPassword() throws Exception {
        // given: the bootstrap user was disabled and their password changed
        enableOAuth();
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(BOOTSTRAP_ID, adminUser().withEmail("admin@kawa").withPassword("forgotten")
                        .withDisplayName("Ops").disabled().build()));
        env = Map.of("KAWA_ADMIN_RESET", "true");

        // when
        startServer();

        // then
        assertThat(signIn("admin@kawa", "s3cret")).hasStatusCode(200);
        AdminUserConfig user = seeded();
        assertThat(user.enabled())
                .withFailMessage(() -> "the reset did not re-enable the bootstrap user")
                .isTrue();
        assertThat(user.displayName()).isEqualTo("Ops");
        assertThat(user.createdAt()).isEqualTo(CREATED_AT);
    }

    @Test
    void resetRecreatesADeletedBootstrapUser() throws Exception {
        // given
        enableOAuth();
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().disabled().build()));
        env = Map.of("KAWA_ADMIN_RESET", "true");

        // when
        startServer();

        // then
        assertThat(signIn("admin@kawa", "s3cret")).hasStatusCode(200);
        assertThat(repository.getActiveConfig().adminUsers())
                .withFailMessage(() -> "the reset dropped the other admin users")
                .containsKey(ALICE_ID)
                .containsKey(BOOTSTRAP_ID);
    }

    @Test
    void oauthStartsWithoutABootstrapUserWhenAdminUsersExist() throws Exception {
        // given
        enableOAuth(null);
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withPassword("alice-secret").build()));

        // when
        startServer();

        // then
        assertThat(signIn("alice@mycom.com", "alice-secret")).hasStatusCode(200);
    }

    @Test
    void oauthRefusesToStartWithoutAnyAdminUser() {
        // given
        enableOAuth(null);

        // when / then
        assertThatThrownBy(this::startServer)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("admin.auth.type 'oauth' requires an admin user: set admin.auth.bootstrapUser to create the first one");
    }

    private AdminUserConfig seeded() {
        AdminUserConfig user = repository.getActiveConfig().adminUsers().get(BOOTSTRAP_ID);
        assertThat(user)
                .withFailMessage(() -> "the bootstrap user was not stored under " + BOOTSTRAP_ID + ": "
                        + repository.getActiveConfig().adminUsers())
                .isNotNull();
        return user;
    }
}
