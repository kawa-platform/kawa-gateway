package io.jonasg.kawa.http.users;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.jonasg.kawa.config.AdminAuthConfig;
import io.jonasg.kawa.config.DynamicConfig;
import io.jonasg.kawa.http.AdminHttpSliceTestBase;
import io.jonasg.kawa.http.FakeDynamicConfigRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static io.jonasg.kawa.http.AdminUserConfigMother.ALICE_ID;
import static io.jonasg.kawa.http.AdminUserConfigMother.adminUser;
import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for `GET /admin/setup`: whether kawa studio has to offer creating the first admin user.
/// It is public, so studio can ask before anyone can sign in.
class GetSetupSliceTest extends AdminHttpSliceTestBase {

    private final Logger setupLog = (Logger) LoggerFactory.getLogger(AdminSetup.class);
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();

    @BeforeEach
    void captureLog() {
        logged.start();
        setupLog.addAppender(logged);
    }

    @AfterEach
    void releaseLog() {
        setupLog.detachAppender(logged);
    }

    @Test
    void requiresSetupWithoutAToken() throws Exception {
        // given: oauth without a bootstrap user, on a gateway without admin users
        enableOAuth(null);
        startServer();

        // when
        var response = send("GET", "/admin/setup");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("""
                {"required":true}
                """);
    }

    @Test
    void staysPublicOnceSetupIsDone() throws Exception {
        // given
        enableOAuth(null);
        repository = new FakeDynamicConfigRepository(DynamicConfig.empty().upsertAdminUser(ALICE_ID, adminUser().build()));
        startServer();

        // when
        var response = send("GET", "/admin/setup");

        // then
        assertThat(response)
                .withFailMessage(() -> "GET /admin/setup did not answer without a token: " + response.statusCode())
                .hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("""
                {"required":false}
                """);
    }

    @Test
    void doesNotRequireSetupWithABootstrapUser() throws Exception {
        // given
        enableOAuth();
        startServer();

        // when
        var response = send("GET", "/admin/setup");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("""
                {"required":false}
                """);
        assertThat(logged.list)
                .withFailMessage(() -> "setup mode was announced although the bootstrap user was seeded: " + logged.list)
                .isEmpty();
    }

    @Test
    void doesNotRequireSetupWithoutAuthentication() throws Exception {
        // given
        auth = AdminAuthConfig.none();
        startServer();

        // when
        var response = send("GET", "/admin/setup");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("""
                {"required":false}
                """);
    }

    @Test
    void logsAGeneratedSetupTokenOnceAtStartup() throws Exception {
        // given
        enableOAuth(null);

        // when
        startServer();

        // then
        assertThat(logged.list)
                .withFailMessage(() -> "setup mode was not announced exactly once at WARN: " + logged.list)
                .singleElement()
                .satisfies(event -> {
                    assertThat(event.getLevel()).isEqualTo(Level.WARN);
                    assertThat(event.getFormattedMessage())
                            .contains("kawa setup: create the first admin user", "KAWA_SETUP_TOKEN", "admin.auth.bootstrapUser")
                            .containsPattern("\n {7}[A-Za-z0-9_-]{43}\n");
                });
    }

    @Test
    void doesNotLogASetupTokenFromTheEnvironment() throws Exception {
        // given
        enableOAuth(null);
        env = Map.of(AdminSetup.TOKEN_ENV, "shared-token-from-a-secret");

        // when
        startServer();

        // then
        assertThat(logged.list)
                .withFailMessage(() -> "setup mode was not announced exactly once: " + logged.list)
                .singleElement()
                .satisfies(event -> assertThat(event.getFormattedMessage())
                        .contains("from the KAWA_SETUP_TOKEN environment variable")
                        .doesNotContain("shared-token-from-a-secret"));
    }
}
