package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AdminAuthConfigTest {

    private final ConfigLoader loader = new ConfigLoader();

    @Test
    void defaultsToNoAuthentication() {
        // when
        GatewayConfig config = loader.loadFromYaml("""
                admin:
                  enabled: true
                """);

        // then
        assertThat(config.admin().auth())
                .withFailMessage(() -> "admin.auth did not default to 'none', was " + config.admin().auth())
                .isEqualTo(AdminAuthConfig.none());
    }

    @Test
    void loadsProviderTypeBootstrapUserAndSettings() {
        // when
        GatewayConfig config = loader.loadFromYaml("""
                admin:
                  enabled: true
                  auth:
                    type: oidc
                    bootstrapUser:
                      username: admin
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
        assertThat(auth.bootstrapUser()).isEqualTo(new AdminAuthConfig.BootstrapUser("admin", "s3cret"));
        assertThat(auth.settings())
                .withFailMessage(() -> "settings were not handed through as-is: " + auth.settings())
                .containsExactlyInAnyOrderEntriesOf(Map.of("issuer", "https://idp.example.com", "tokenTtl", "PT5M"));
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
        var user = AdminAuthConfig.BootstrapUser.of("admin", "${KAWA_ADMIN_PASSWORD}",
                Map.of("KAWA_ADMIN_PASSWORD", "from-env")::get);

        // then
        assertThat(user.password())
                .withFailMessage(() -> "bootstrap password was not resolved from the environment")
                .isEqualTo("from-env");
    }

    @Test
    void neverSerializesAuthSoSecretsStayOutOfTheConfigTopic() {
        // given
        var admin = new AdminConfig(true, null, null, null, new AdminAuthConfig("oauth",
                new AdminAuthConfig.BootstrapUser("admin", "s3cret"), Map.of("signingKey", "private-pem")));

        // when
        String json = JsonMapper.builder().build().writeValueAsString(admin);

        // then
        assertThat(json)
                .withFailMessage(() -> "admin.auth was serialized: " + json)
                .doesNotContain("auth")
                .doesNotContain("s3cret")
                .doesNotContain("private-pem");
    }
}
