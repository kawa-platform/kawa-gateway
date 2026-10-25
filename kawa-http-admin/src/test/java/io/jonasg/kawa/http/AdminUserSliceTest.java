package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminUserConfig;
import io.jonasg.kawa.config.DynamicConfig;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static io.jonasg.kawa.http.AdminUserConfigMother.ALICE_ID;
import static io.jonasg.kawa.http.AdminUserConfigMother.BOB_ID;
import static io.jonasg.kawa.http.AdminUserConfigMother.CREATED_AT;
import static io.jonasg.kawa.http.AdminUserConfigMother.adminUser;
import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for the `/admin/users` admin surface. Admin users are created with `POST`, get a
/// gateway-assigned UUID, and are read, updated (`PATCH`) and deleted by that id. Their email is
/// their username: unique, case-insensitive and stored in lower case.
class AdminUserSliceTest extends AdminHttpSliceTestBase {

    // --- GET /admin/users ---------------------------------------------------------------------

    @Test
    void listsAdminUsersSortedByEmailWithoutHashes() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(BOB_ID, adminUser().withEmail("bob@mycom.com").disabled().build())
                .upsertAdminUser(ALICE_ID, adminUser().withEmail("alice@mycom.com").withDisplayName("Alice Liddell").build()));
        startServer();

        // when
        var response = send("GET", "/admin/users");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("""
                [
                  {"id":"%s","email":"alice@mycom.com","displayName":"Alice Liddell","enabled":true,"createdAt":"2026-01-01T00:00:00Z"},
                  {"id":"%s","email":"bob@mycom.com","displayName":null,"enabled":false,"createdAt":"2026-01-01T00:00:00Z"}
                ]
                """.formatted(ALICE_ID, BOB_ID));
    }

    // --- POST /admin/users --------------------------------------------------------------------

    @Test
    void createsAdminUserWithAGatewayAssignedId() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/admin/users", """
                {"email":"alice@mycom.com","password":"alice-secret","displayName":"Alice Liddell"}
                """);

        // then
        assertThat(response).hasStatusCode(201);
        String id = json(response).get("id").asString();
        assertThat(UUID.fromString(id).toString())
                .withFailMessage(() -> "the admin user id is not a UUID: " + id)
                .isEqualTo(id);
        assertThatJson(response.body()).isEqualTo("""
                {"id":"%s","email":"alice@mycom.com","displayName":"Alice Liddell","enabled":true,"createdAt":"${json-unit.any-string}"}
                """.formatted(id));
        assertThat(response.headers().firstValue("Location"))
                .withFailMessage(() -> "the created admin user has no Location: " + response.headers())
                .hasValue("/admin/users/" + id);
        assertThat(stored(id).password().verify("alice-secret"))
                .withFailMessage(() -> "the stored admin user does not verify with their password")
                .isTrue();
    }

    @Test
    void createsEveryAdminUserWithANewId() throws Exception {
        // given
        startServer();

        // when
        var alice = send("POST", "/admin/users", """
                {"email":"alice@mycom.com","password":"alice-secret"}
                """);
        var bob = send("POST", "/admin/users", """
                {"email":"bob@mycom.com","password":"bob-secret"}
                """);

        // then
        assertThat(json(alice).get("id").asString())
                .withFailMessage(() -> "two admin users got the same id")
                .isNotEqualTo(json(bob).get("id").asString());
        assertThat(repository.getActiveConfig().adminUsers()).hasSize(2);
    }

    @Test
    void createdAdminUserIsEnabledWithoutDisplayName() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/admin/users", """
                {"email":"alice@mycom.com","password":"alice-secret"}
                """);

        // then
        assertThat(response).hasStatusCode(201);
        assertThatJson(response.body()).isObject()
                .containsEntry("enabled", true)
                .containsEntry("displayName", null);
    }

    @Test
    void createdAtIsAssignedByTheGateway() throws Exception {
        // given
        startServer();
        Instant before = Instant.now().truncatedTo(ChronoUnit.SECONDS);

        // when
        var response = send("POST", "/admin/users", """
                {"email":"alice@mycom.com","password":"alice-secret"}
                """);

        // then
        Instant after = Instant.now();
        assertThat(response).hasStatusCode(201);
        Instant createdAt = Instant.parse(json(response).get("createdAt").asString());
        assertThat(createdAt)
                .withFailMessage(() -> "createdAt " + createdAt + " was not assigned during the request [" + before + ", " + after + "]")
                .isBetween(before, after);
    }

    @Test
    void storesTheEmailInLowerCase() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/admin/users", """
                {"email":"Alice@MyCom.com","password":"alice-secret"}
                """);

        // then
        assertThat(response).hasStatusCode(201);
        assertThatJson(response.body()).isObject().containsEntry("email", "alice@mycom.com");
    }

    @Test
    void rejectsAnEmailThatIsAlreadyInUseIgnoringCase() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withEmail("alice@mycom.com").build()));
        startServer();

        // when
        var response = send("POST", "/admin/users", """
                {"email":"ALICE@mycom.com","password":"other-secret"}
                """);

        // then
        assertThat(response).hasStatusCode(409);
        assertThatJson(response.body()).isEqualTo("""
                {"error":"admin user email 'alice@mycom.com' is already in use"}
                """);
        assertThat(repository.getActiveConfig().adminUsers())
                .withFailMessage(() -> "a second admin user with the same email was stored")
                .hasSize(1);
    }

    @Test
    void rejectsTheSeededBootstrapUsersEmail() throws Exception {
        // given
        enableOAuth();
        startServer();
        String token = login("admin@kawa", "s3cret");

        // when
        var response = send("POST", "/admin/users", """
                {"email":"Admin@Kawa","password":"other-secret"}
                """, bearer(token));

        // then
        assertThat(response).hasStatusCode(409);
        assertThatJson(response.body()).isEqualTo("""
                {"error":"admin user email 'admin@kawa' is already in use"}
                """);
    }

    @Test
    void rejectsAnInvalidEmail() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/admin/users", """
                {"email":"alice","password":"alice-secret"}
                """);

        // then
        assertThat(response).hasStatusCode(400);
        assertThatJson(response.body()).isEqualTo("""
                {"error":"'alice' is not an email address"}
                """);
        assertThat(repository.getActiveConfig().adminUsers()).isEmpty();
    }

    @Test
    void rejectsCreatingWithoutEmail() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/admin/users", """
                {"password":"alice-secret"}
                """);

        // then
        assertThat(response).hasStatusCode(400);
        assertThatJson(response.body()).isEqualTo("""
                {"error":"admin user: email is required"}
                """);
    }

    @Test
    void rejectsCreatingWithoutPassword() throws Exception {
        // given
        startServer();

        // when
        var response = send("POST", "/admin/users", """
                {"email":"alice@mycom.com"}
                """);

        // then
        assertThat(response).hasStatusCode(400);
        assertThatJson(response.body()).isEqualTo("""
                {"error":"admin user 'alice@mycom.com': password is required"}
                """);
    }

    @Test
    void rejectsFieldsTheClientCannotSetOnCreate() throws Exception {
        // given
        startServer();

        // when
        var enabled = send("POST", "/admin/users", """
                {"email":"alice@mycom.com","password":"alice-secret","enabled":false}
                """);
        var createdAt = send("POST", "/admin/users", """
                {"email":"alice@mycom.com","password":"alice-secret","createdAt":"2000-01-01T00:00:00Z"}
                """);

        // then
        assertThat(enabled).hasStatusCode(400);
        assertThat(createdAt).hasStatusCode(400);
        assertThat(repository.getActiveConfig().adminUsers())
                .withFailMessage(() -> "a create with fields the gateway owns was stored")
                .isEmpty();
    }

    // --- GET /admin/users/{id} ----------------------------------------------------------------

    @Test
    void getsAdminUserById() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withDisplayName("Alice Liddell").build()));
        startServer();

        // when
        var response = send("GET", "/admin/users/" + ALICE_ID);

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("""
                {"id":"%s","email":"alice@mycom.com","displayName":"Alice Liddell","enabled":true,"createdAt":"2026-01-01T00:00:00Z"}
                """.formatted(ALICE_ID));
    }

    @Test
    void unknownOrMalformedIdIsNotFound() throws Exception {
        // given
        startServer();

        // when
        var unknown = send("GET", "/admin/users/" + UUID.randomUUID());
        var malformed = send("GET", "/admin/users/alice@mycom.com");

        // then
        assertThat(unknown).hasStatusCode(404);
        assertThat(malformed).hasStatusCode(404);
    }

    // --- PATCH /admin/users/{id} --------------------------------------------------------------

    @Test
    void disablesAdminUserWithoutTheirPassword() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withPassword("alice-secret").withDisplayName("Alice Liddell").build())
                .upsertAdminUser(BOB_ID, adminUser().withEmail("bob@mycom.com").build()));
        startServer();

        // when
        var response = send("PATCH", "/admin/users/" + ALICE_ID, """
                {"enabled":false}
                """);

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("""
                {"id":"%s","email":"alice@mycom.com","displayName":"Alice Liddell","enabled":false,"createdAt":"2026-01-01T00:00:00Z"}
                """.formatted(ALICE_ID));
        assertThat(stored(ALICE_ID).password().verify("alice-secret"))
                .withFailMessage(() -> "disabling the admin user changed their password")
                .isTrue();
    }

    @Test
    void reEnablesDisabledAdminUser() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().disabled().build()));
        startServer();

        // when
        var response = send("PATCH", "/admin/users/" + ALICE_ID, """
                {"enabled":true}
                """);

        // then
        assertThat(response).hasStatusCode(200);
        assertThat(stored(ALICE_ID).enabled())
                .withFailMessage(() -> "the admin user was not re-enabled")
                .isTrue();
    }

    @Test
    void changesDisplayNameWithoutThePassword() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withPassword("alice-secret").withDisplayName("Alice").build()));
        startServer();

        // when
        var response = send("PATCH", "/admin/users/" + ALICE_ID, """
                {"displayName":"Alice Liddell"}
                """);

        // then
        assertThat(response).hasStatusCode(200);
        AdminUserConfig stored = stored(ALICE_ID);
        assertThat(stored.displayName())
                .withFailMessage(() -> "the display name was not changed: " + stored)
                .isEqualTo("Alice Liddell");
        assertThat(stored.password().verify("alice-secret"))
                .withFailMessage(() -> "changing the display name changed the password")
                .isTrue();
        assertThat(stored.enabled())
                .withFailMessage(() -> "changing the display name disabled the admin user")
                .isTrue();
    }

    @Test
    void clearsDisplayNameWithAnEmptyValue() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withDisplayName("Alice").build()));
        startServer();

        // when
        var response = send("PATCH", "/admin/users/" + ALICE_ID, """
                {"displayName":""}
                """);

        // then
        assertThat(response).hasStatusCode(200);
        assertThat(stored(ALICE_ID).displayName())
                .withFailMessage(() -> "the display name was not cleared")
                .isNull();
    }

    @Test
    void changesThePassword() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withPassword("alice-secret").build()));
        startServer();

        // when
        var response = send("PATCH", "/admin/users/" + ALICE_ID, """
                {"password":"new-secret"}
                """);

        // then
        assertThat(response).hasStatusCode(200);
        assertThat(stored(ALICE_ID).password().verify("new-secret"))
                .withFailMessage(() -> "the password was not changed")
                .isTrue();
    }

    @Test
    void changesTheEmailInLowerCase() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withEmail("alice@mycom.com").build()));
        startServer();

        // when
        var response = send("PATCH", "/admin/users/" + ALICE_ID, """
                {"email":"Alice.Liddell@MyCom.com"}
                """);

        // then
        assertThat(response).hasStatusCode(200);
        assertThat(stored(ALICE_ID).email())
                .withFailMessage(() -> "the email was not changed")
                .isEqualTo("alice.liddell@mycom.com");
    }

    @Test
    void keepsItsOwnEmailInADifferentCase() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withEmail("alice@mycom.com").build()));
        startServer();

        // when
        var response = send("PATCH", "/admin/users/" + ALICE_ID, """
                {"email":"ALICE@mycom.com"}
                """);

        // then
        assertThat(response).hasStatusCode(200);
    }

    @Test
    void rejectsChangingToAnEmailInUseIgnoringCase() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withEmail("alice@mycom.com").build())
                .upsertAdminUser(BOB_ID, adminUser().withEmail("bob@mycom.com").build()));
        startServer();

        // when
        var response = send("PATCH", "/admin/users/" + BOB_ID, """
                {"email":"Alice@MyCom.com"}
                """);

        // then
        assertThat(response).hasStatusCode(409);
        assertThatJson(response.body()).isEqualTo("""
                {"error":"admin user email 'alice@mycom.com' is already in use"}
                """);
        assertThat(stored(BOB_ID).email()).isEqualTo("bob@mycom.com");
    }

    @Test
    void rejectsABlankPassword() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withPassword("alice-secret").build()));
        startServer();

        // when
        var response = send("PATCH", "/admin/users/" + ALICE_ID, """
                {"password":"  "}
                """);

        // then
        assertThat(response).hasStatusCode(400);
        assertThatJson(response.body()).isEqualTo("""
                {"error":"admin user 'alice@mycom.com': password must not be blank"}
                """);
        assertThat(stored(ALICE_ID).password().verify("alice-secret"))
                .withFailMessage(() -> "a rejected update changed the password")
                .isTrue();
    }

    @Test
    void rejectsUnknownFieldsOnUpdate() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().build()));
        startServer();

        // when
        var typo = send("PATCH", "/admin/users/" + ALICE_ID, """
                {"enable":false}
                """);
        var createdAt = send("PATCH", "/admin/users/" + ALICE_ID, """
                {"createdAt":"2000-01-01T00:00:00Z"}
                """);

        // then
        assertThat(typo).hasStatusCode(400);
        assertThat(createdAt).hasStatusCode(400);
        AdminUserConfig stored = stored(ALICE_ID);
        assertThat(stored.enabled())
                .withFailMessage(() -> "a rejected update disabled the admin user")
                .isTrue();
        assertThat(stored.createdAt())
                .withFailMessage(() -> "a rejected update changed createdAt")
                .isEqualTo(CREATED_AT);
    }

    @Test
    void keepsCreatedAtOnUpdate() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().build()));
        startServer();

        // when
        var response = send("PATCH", "/admin/users/" + ALICE_ID, """
                {"displayName":"Alice Liddell","password":"new-secret"}
                """);

        // then
        assertThat(response).hasStatusCode(200);
        assertThat(stored(ALICE_ID).createdAt())
                .withFailMessage(() -> "updating the admin user changed createdAt")
                .isEqualTo(CREATED_AT);
    }

    @Test
    void updatingUnknownAdminUserIsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("PATCH", "/admin/users/" + UUID.randomUUID(), """
                {"enabled":false}
                """);

        // then
        assertThat(response).hasStatusCode(404);
    }

    // --- DELETE /admin/users/{id} -------------------------------------------------------------

    @Test
    void deletesAdminUser() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().build())
                .upsertAdminUser(BOB_ID, adminUser().withEmail("bob@mycom.com").build()));
        startServer();

        // when
        var response = send("DELETE", "/admin/users/" + ALICE_ID);

        // then
        assertThat(response).hasStatusCode(204);
        assertThat(repository.getActiveConfig().adminUsers())
                .withFailMessage(() -> "the admin user was not removed")
                .doesNotContainKey(ALICE_ID);
    }

    @Test
    void deletingUnknownAdminUserIsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("DELETE", "/admin/users/" + UUID.randomUUID());

        // then
        assertThat(response).hasStatusCode(404);
    }

    // --- lockout rails ------------------------------------------------------------------------

    @Test
    void refusesToDeleteYourOwnAdminUser() throws Exception {
        // given
        enableOAuth();
        repository = new FakeDynamicConfigRepository(aliceAndBob());
        startServer();
        String aliceToken = login("alice@mycom.com", "alice-secret");

        // when
        var response = send("DELETE", "/admin/users/" + ALICE_ID, null, bearer(aliceToken));

        // then
        assertThat(response).hasStatusCode(409);
        assertThatJson(response.body()).isEqualTo("""
                {"error":"you cannot delete your own admin user 'alice@mycom.com'; sign in as another admin user to delete it"}
                """);
        assertThat(repository.getActiveConfig().adminUsers())
                .withFailMessage(() -> "the caller's own admin user was deleted")
                .containsKey(ALICE_ID);
    }

    @Test
    void refusesToDisableYourOwnAdminUser() throws Exception {
        // given
        enableOAuth();
        repository = new FakeDynamicConfigRepository(aliceAndBob());
        startServer();
        String aliceToken = login("alice@mycom.com", "alice-secret");

        // when
        var response = send("PATCH", "/admin/users/" + ALICE_ID, """
                {"enabled":false}
                """, bearer(aliceToken));

        // then
        assertThat(response).hasStatusCode(409);
        assertThatJson(response.body()).isEqualTo("""
                {"error":"you cannot disable your own admin user 'alice@mycom.com'; sign in as another admin user to disable it"}
                """);
        assertThat(stored(ALICE_ID).enabled())
                .withFailMessage(() -> "the caller's own admin user was disabled")
                .isTrue();
    }

    @Test
    void refusesToDeleteTheLastEnabledAdminUser() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().build())
                .upsertAdminUser(BOB_ID, adminUser().withEmail("bob@mycom.com").disabled().build()));
        startServer();

        // when
        var response = send("DELETE", "/admin/users/" + ALICE_ID);

        // then
        assertThat(response).hasStatusCode(409);
        assertThatJson(response.body()).isEqualTo("""
                {"error":"admin user 'alice@mycom.com' is the last enabled admin user; create or enable another admin user before you delete it"}
                """);
        assertThat(repository.getActiveConfig().adminUsers())
                .withFailMessage(() -> "the last enabled admin user was deleted")
                .containsKey(ALICE_ID);
    }

    @Test
    void refusesToDisableTheLastEnabledAdminUser() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().build()));
        startServer();

        // when
        var response = send("PATCH", "/admin/users/" + ALICE_ID, """
                {"enabled":false}
                """);

        // then
        assertThat(response).hasStatusCode(409);
        assertThatJson(response.body()).isEqualTo("""
                {"error":"admin user 'alice@mycom.com' is the last enabled admin user; create or enable another admin user before you disable it"}
                """);
        assertThat(stored(ALICE_ID).enabled())
                .withFailMessage(() -> "the last enabled admin user was disabled")
                .isTrue();
    }

    @Test
    void deletesADisabledAdminUserBesideTheLastEnabledOne() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().build())
                .upsertAdminUser(BOB_ID, adminUser().withEmail("bob@mycom.com").disabled().build()));
        startServer();

        // when
        var response = send("DELETE", "/admin/users/" + BOB_ID);

        // then
        assertThat(response).hasStatusCode(204);
        assertThat(repository.getActiveConfig().adminUsers())
                .withFailMessage(() -> "the disabled admin user was not removed")
                .doesNotContainKey(BOB_ID);
    }

    @Test
    void concurrentDeletesCannotRemoveEveryEnabledAdminUser() throws Exception {
        // given
        repository = new FakeDynamicConfigRepository(aliceAndBob());
        startServer();

        // when
        var deleteAlice = CompletableFuture.supplyAsync(() -> deleteQuietly(ALICE_ID));
        var deleteBob = CompletableFuture.supplyAsync(() -> deleteQuietly(BOB_ID));
        List<Integer> statuses = List.of(deleteAlice.get().statusCode(), deleteBob.get().statusCode());

        // then
        assertThat(statuses)
                .withFailMessage(() -> "expected one delete to succeed and one to be refused, got " + statuses)
                .containsExactlyInAnyOrder(204, 409);
        assertThat(repository.getActiveConfig().adminUsers())
                .withFailMessage(() -> "concurrent deletes left no admin user")
                .hasSize(1);
    }

    // --- secrets ------------------------------------------------------------------------------

    @Test
    void responsesNeverContainPasswordOrHash() throws Exception {
        // given
        startServer();

        // when
        var created = send("POST", "/admin/users", """
                {"email":"alice@mycom.com","password":"alice-secret","displayName":"Alice Liddell"}
                """);
        String id = json(created).get("id").asString();
        var updated = send("PATCH", "/admin/users/" + id, """
                {"password":"new-secret"}
                """);
        var one = send("GET", "/admin/users/" + id);
        var list = send("GET", "/admin/users");

        // then
        String hash = stored(id).password().encoded();
        for (var response : List.of(created, updated, one, list)) {
            assertThat(response.body())
                    .withFailMessage(() -> "an admin user response leaked the password or its hash: " + response.body())
                    .doesNotContain("password")
                    .doesNotContain("alice-secret")
                    .doesNotContain("new-secret")
                    .doesNotContain(hash);
        }
    }

    /// Alice (`alice-secret`) and Bob (`bob-secret`), both enabled.
    private static DynamicConfig aliceAndBob() {
        return DynamicConfig.empty()
                .upsertAdminUser(ALICE_ID, adminUser().withPassword("alice-secret").build())
                .upsertAdminUser(BOB_ID, adminUser().withEmail("bob@mycom.com").withPassword("bob-secret").build());
    }

    private HttpResponse<String> deleteQuietly(String id) {
        try {
            return send("DELETE", "/admin/users/" + id);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private AdminUserConfig stored(String id) {
        AdminUserConfig user = repository.getActiveConfig().adminUsers().get(id);
        assertThat(user)
                .withFailMessage(() -> "no admin user stored under " + id + ": " + repository.getActiveConfig().adminUsers())
                .isNotNull();
        return user;
    }

    private static JsonNode json(HttpResponse<String> response) {
        return JsonMapper.builder().build().readTree(response.body());
    }
}
