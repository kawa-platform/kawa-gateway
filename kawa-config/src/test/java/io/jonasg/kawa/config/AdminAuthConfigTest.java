package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AdminAuthConfigTest {

    private final ConfigLoader loader = new ConfigLoader();

    @Test
    void defaultsToOAuth() {
        // when
        BootstrapConfig config = loader.loadFromYaml("""
                admin:
                  enabled: true
                """);

        // then
        assertThat(config.admin().auth().type())
                .withFailMessage(() -> "admin.auth.type did not default to 'oauth', was " + config.admin().auth().type())
                .isEqualTo("oauth");
    }

    @Test
    void blankTypeDefaultsToOAuth() {
        // when
        BootstrapConfig config = loader.loadFromYaml("""
                admin:
                  enabled: true
                  auth:
                    type: " "
                """);

        // then
        assertThat(config.admin().auth().type())
                .withFailMessage(() -> "a blank admin.auth.type did not default to 'oauth', was " + config.admin().auth().type())
                .isEqualTo("oauth");
    }

    @Test
    void keepsExplicitNone() {
        // when
        BootstrapConfig config = loader.loadFromYaml("""
                admin:
                  enabled: true
                  auth:
                    type: none
                """);

        // then
        assertThat(config.admin().auth())
                .withFailMessage(() -> "an explicit admin.auth.type 'none' was not kept, was " + config.admin().auth())
                .isEqualTo(AdminAuthConfig.none());
    }

    @Test
    void loadsProviderTypeBootstrapUserAndSettings() {
        // when
        BootstrapConfig config = loader.loadFromYaml("""
                admin:
                  enabled: true
                  auth:
                    type: oidc
                    bootstrapUser:
                      username: admin@kawa
                      password: s3cret
                    settings:
                      issuer: https://idp.example.com
                      tokenTtl: PT5M
                """);

        // then
        AdminAuthConfig auth = config.admin().auth();
        assertThat(auth.type())
                .withFailMessage(() -> "an arbitrary provider name was not kept: " + auth.type())
                .isEqualTo("oidc");
        assertThat(auth.bootstrapUser()).isEqualTo(new AdminAuthConfig.BootstrapUser("admin@kawa", "s3cret"));
        assertThat(auth.settings())
                .withFailMessage(() -> "settings were not handed through as-is: " + auth.settings())
                .containsExactlyInAnyOrderEntriesOf(Map.of("issuer", "https://idp.example.com", "tokenTtl", "PT5M"));
    }

    @Test
    void rejectsBootstrapUsernameThatIsNotAnEmail() {
        // when / then
        assertThatThrownBy(() -> new AdminAuthConfig.BootstrapUser("admin", "s3cret"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("admin.auth.bootstrapUser.username must be an email address");
    }

    @Test
    void resolvesEnvironmentPlaceholdersInSettings() {
        // when
        var settings = AdminAuthConfig.resolveEnvVars(
                Map.of("signingKey", "${KAWA_ADMIN_SIGNING_KEY}", "issuer", "${KAWA_ISSUER:-kawa}"),
                Map.of("KAWA_ADMIN_SIGNING_KEY", "pem-from-env")::get);

        // then
        assertThat(settings)
                .withFailMessage(() -> "settings placeholders were not resolved: " + settings)
                .containsEntry("signingKey", "pem-from-env")
                .containsEntry("issuer", "kawa");
    }

    @Test
    void resolvesBootstrapPasswordFromEnvironment() {
        // when
        var user = AdminAuthConfig.BootstrapUser.of("admin@kawa", "${KAWA_ADMIN_PASSWORD}",
                Map.of("KAWA_ADMIN_PASSWORD", "from-env")::get);

        // then
        assertThat(user.password())
                .withFailMessage(() -> "bootstrap password was not resolved from the environment")
                .isEqualTo("from-env");
    }
}
