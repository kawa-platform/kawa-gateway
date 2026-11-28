package io.jonasg.kawa.http.users;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.jonasg.kawa.config.AdminAuthConfig;
import io.jonasg.kawa.config.DynamicConfig;
import io.jonasg.kawa.http.AdminHttpServer;
import io.jonasg.kawa.http.AdminHttpSliceTestBase;
import io.jonasg.kawa.http.FakeDynamicConfigRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

import static io.jonasg.kawa.http.AdminUserConfigMother.ALICE_ID;
import static io.jonasg.kawa.http.AdminUserConfigMother.adminUser;
import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for `POST /admin/setup`: kawa studio creates the first admin user of a gateway in setup
/// mode with the setup token the gateway logged (or took from `KAWA_SETUP_TOKEN`).
class PostSetupSliceTest extends AdminHttpSliceTestBase {

    private static final String TOKEN = "shared-setup-token";
    private static final String NOT_REQUIRED = "setup is not required: the gateway is not in setup mode or already has an admin user";

    @Test
    void createsTheFirstAdminUser() throws Exception {
        // given
        inSetupMode();
        startServer();

        // when
        var setupResp = send("POST", "/admin/setup", setupBody(TOKEN, "Alice@MyCom.com", "alice-secret"), json());

        // then
        assertThat(setupResp).hasStatusCode(201);
        var stored = repository.getActiveConfig().adminUsers();
        assertThat(stored)
                .withFailMessage(() -> "setup did not store exactly one admin user: " + stored)
                .hasSize(1);
        String id = stored.keySet().iterator().next();
        assertThatJson(setupResp.body()).whenIgnoringPaths("createdAt").isEqualTo("""
                {"id":"%s","email":"alice@mycom.com","displayName":null,"enabled":true}
                """.formatted(id));
        assertThatJson(setupResp.body()).node("createdAt").isString();
        assertThat(setupResp.headers().firstValue("Location")).contains("/admin/users/" + id);
        assertThat(stored.get(id).password().encoded())
                .withFailMessage(() -> "the setup password was stored in plaintext")
                .doesNotContain("alice-secret");
    }

    @Test
    void theCreatedAdminUserCanSignIn() throws Exception {
        // given
        inSetupMode();
        startServer();
        send("POST", "/admin/setup", setupBody(TOKEN, "alice@mycom.com", "alice-secret"), json());

        // when
        var usersResp = send("GET", "/admin/users", null, bearer(login("alice@mycom.com", "alice-secret")));

        // then
        assertThat(usersResp).hasStatusCode(200);
        assertThatJson(usersResp.body()).node("[0].email").isEqualTo("alice@mycom.com");
    }

    @Test
    void acceptsTheTokenTheGatewayLogged() throws Exception {
        // given
        enableOAuth(null);
        var setupLog = (Logger) LoggerFactory.getLogger(AdminSetup.class);
        var logged = new ListAppender<ILoggingEvent>();
        logged.start();
        setupLog.addAppender(logged);
        try {
            startServer();
        } finally {
            setupLog.detachAppender(logged);
        }
        var matcher = Pattern.compile("setup token:\\s+([A-Za-z0-9_-]+)").matcher(logged.list.getFirst().getFormattedMessage());
        assertThat(matcher.find()).withFailMessage(() -> "no setup token was logged: " + logged.list).isTrue();

        // when
        var setupResp = send("POST", "/admin/setup", setupBody(matcher.group(1), "alice@mycom.com", "alice-secret"), json());

        // then
        assertThat(setupResp).hasStatusCode(201);
    }

    @Test
    void setupIsNoLongerRequiredAfterwards() throws Exception {
        // given
        inSetupMode();
        startServer();
        send("POST", "/admin/setup", setupBody(TOKEN, "alice@mycom.com", "alice-secret"), json());

        // when
        var statusResp = send("GET", "/admin/setup");

        // then
        assertThatJson(statusResp.body()).isEqualTo("""
                {"required":false}
                """);
    }

    @Test
    void refusesASecondSetup() throws Exception {
        // given
        inSetupMode();
        startServer();
        assertThat(send("POST", "/admin/setup", setupBody(TOKEN, "alice@mycom.com", "alice-secret"), json())).hasStatusCode(201);

        // when
        var secondResp = send("POST", "/admin/setup", setupBody(TOKEN, "mallory@mycom.com", "mallory-secret"), json());

        // then
        assertThat(secondResp).hasStatusCode(409);
        assertThatJson(secondResp.body()).isEqualTo("""
                {"error":"%s"}
                """.formatted(NOT_REQUIRED));
        assertThat(repository.getActiveConfig().adminUsers())
                .withFailMessage(() -> "a second setup stored another admin user")
                .hasSize(1);
    }

    @Test
    void rejectsAWrongToken() throws Exception {
        // given
        inSetupMode();
        startServer();

        // when
        var setupResp = send("POST", "/admin/setup", setupBody("guessed", "alice@mycom.com", "alice-secret"), json());

        // then
        assertThat(setupResp).hasStatusCode(401);
        assertThatJson(setupResp.body()).isEqualTo("""
                {"error":"invalid setup token"}
                """);
        assertNoAdminUserStored();
    }

    @Test
    void rejectsAMissingToken() throws Exception {
        // given
        inSetupMode();
        startServer();

        // when
        var setupResp = send("POST", "/admin/setup", """
                {"email":"alice@mycom.com","password":"alice-secret"}
                """, json());

        // then
        assertThat(setupResp).hasStatusCode(401);
        assertThatJson(setupResp.body()).isEqualTo("""
                {"error":"invalid setup token"}
                """);
        assertNoAdminUserStored();
    }

    @Test
    void checksTheTokenBeforeTheAdminUser() throws Exception {
        // given
        inSetupMode();
        startServer();

        // when
        var setupResp = send("POST", "/admin/setup", setupBody("guessed", "not-an-email", ""), json());

        // then
        assertThat(setupResp)
                .withFailMessage(() -> "an unauthenticated caller got feedback on the admin user: " + setupResp.body())
                .hasStatusCode(401);
    }

    @Test
    void rejectsAnInvalidEmail() throws Exception {
        // given
        inSetupMode();
        startServer();

        // when
        var setupResp = send("POST", "/admin/setup", setupBody(TOKEN, "not-an-email", "alice-secret"), json());

        // then
        assertThat(setupResp).hasStatusCode(400);
        assertThatJson(setupResp.body()).isEqualTo("""
                {"error":"'not-an-email' is not an email address"}
                """);
        assertNoAdminUserStored();
    }

    @Test
    void rejectsABlankPassword() throws Exception {
        // given
        inSetupMode();
        startServer();

        // when
        var setupResp = send("POST", "/admin/setup", setupBody(TOKEN, "alice@mycom.com", " "), json());

        // then
        assertThat(setupResp).hasStatusCode(400);
        assertThatJson(setupResp.body()).isEqualTo("""
                {"error":"admin user 'alice@mycom.com': password is required"}
                """);
        assertNoAdminUserStored();
    }

    @Test
    void rejectsAMalformedBody() throws Exception {
        // given
        inSetupMode();
        startServer();

        // when
        var setupResp = send("POST", "/admin/setup", """
                {"token":"%s","email":"alice@mycom.com","password":"alice-secret","enabled":false}
                """.formatted(TOKEN), json());

        // then
        assertThat(setupResp).hasStatusCode(400);
        assertThatJson(setupResp.body()).node("error").isString().startsWith("invalid setup body: ");
        assertThat(setupResp.body())
                .withFailMessage(() -> "the error echoed the setup token: " + setupResp.body())
                .doesNotContain(TOKEN);
        assertNoAdminUserStored();
    }

    @Test
    void refusesSetupWhenAdminUsersExist() throws Exception {
        // given
        inSetupMode();
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().upsertAdminUser(ALICE_ID, adminUser().build()));
        startServer();

        // when
        var setupResp = send("POST", "/admin/setup", setupBody(TOKEN, "mallory@mycom.com", "mallory-secret"), json());

        // then
        assertThat(setupResp).hasStatusCode(409);
        assertThat(repository.getActiveConfig().adminUsers()).containsOnlyKeys(ALICE_ID);
    }

    @Test
    void refusesSetupWithABootstrapUser() throws Exception {
        // given
        enableOAuth();
        env = Map.of(AdminSetup.TOKEN_ENV, TOKEN);
        startServer();

        // when
        var setupResp = send("POST", "/admin/setup", setupBody(TOKEN, "mallory@mycom.com", "mallory-secret"), json());

        // then
        assertThat(setupResp).hasStatusCode(409);
    }

    @Test
    void refusesSetupWithoutAuthentication() throws Exception {
        // given
        auth = AdminAuthConfig.none();
        env = Map.of(AdminSetup.TOKEN_ENV, TOKEN);
        startServer();

        // when
        var setupResp = send("POST", "/admin/setup", setupBody(TOKEN, "mallory@mycom.com", "mallory-secret"), json());

        // then
        assertThat(setupResp).hasStatusCode(409);
        assertNoAdminUserStored();
    }

    @Test
    void createsExactlyOneAdminUserAcrossConcurrentInstances() throws Exception {
        // given: two instances in setup mode sharing the config and KAWA_SETUP_TOKEN
        inSetupMode();
        startServer();
        AdminHttpServer second = startAnotherServer();
        var start = new CountDownLatch(1);
        var attempts = new ArrayList<CompletableFuture<HttpResponse<String>>>();
        for (int i = 0; i < 10; i++) {
            attempts.add(setupAsync(start, null, "first-" + i + "@mycom.com"));
            attempts.add(setupAsync(start, second, "second-" + i + "@mycom.com"));
        }

        // when
        start.countDown();

        // then
        List<Integer> statuses = attempts.stream().map(attempt -> attempt.join().statusCode()).toList();
        assertThat(statuses)
                .withFailMessage(() -> "not exactly one concurrent setup succeeded: " + statuses)
                .containsOnlyOnce(201)
                .containsOnly(201, 409);
        assertThat(repository.getActiveConfig().adminUsers())
                .withFailMessage(() -> "concurrent setups did not store exactly one admin user")
                .hasSize(1);
        assertThatJson(send(second, "GET", "/admin/setup", null, Map.of()).body()).isEqualTo("""
                {"required":false}
                """);
    }

    private void inSetupMode() {
        enableOAuth(null);
        env = Map.of(AdminSetup.TOKEN_ENV, TOKEN);
    }

    private CompletableFuture<HttpResponse<String>> setupAsync(CountDownLatch start, AdminHttpServer instance, String email) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                start.await();
                String body = setupBody(TOKEN, email, "secret");
                return instance == null ? send("POST", "/admin/setup", body, json()) : send(instance, "POST", "/admin/setup", body, json());
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private void assertNoAdminUserStored() {
        assertThat(repository.getActiveConfig().adminUsers())
                .withFailMessage(() -> "a refused setup stored an admin user: " + repository.getActiveConfig().adminUsers())
                .isEmpty();
    }

    private static String setupBody(String token, String email, String password) {
        return JsonMapper.builder().build().writeValueAsString(Map.of("token", token, "email", email, "password", password));
    }

    private static Map<String, String> json() {
        return Map.of("Content-Type", "application/json");
    }
}
