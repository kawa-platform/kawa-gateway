package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminAuthConfig;
import io.jonasg.kawa.http.auth.builtin.BuiltInAuthenticationPlugin;
import io.jonasg.kawa.plugin.KawaPlugin;
import io.jonasg.kawa.plugin.KawaPluginContext;
import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import io.jonasg.kawa.plugin.http.HttpResponse;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static io.jonasg.kawa.test.KawaAssertions.assertThatThrownBy;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/// Slice tests for authentication providers contributed by plugins: a provider that only uses the
/// public plugin API is selected by `admin.auth.type` exactly like the built-ins.
class PluginAuthenticationProviderSliceTest extends AdminHttpSliceTestBase {

    @Test
    void selectsAProviderRegisteredByAPlugin() throws Exception {
        // given
        useApiKeyProvider();
        startServer();

        // when
        var withoutKey = send("GET", "/topics");
        var withKey = send("GET", "/topics", null, Map.of("X-Api-Key", "letmein"));

        // then
        assertThat(withoutKey).hasStatusCode(401);
        assertThat(withKey).hasStatusCode(200);
    }

    @Test
    void servesThePublicEndpointsThePluginRegisters() throws Exception {
        // given
        useApiKeyProvider();
        startServer();

        // when
        var response = send("GET", "/test/hello");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body()).isEqualTo("""
                {"hello":"from the api-key plugin"}
                """);
    }

    @Test
    void failsToStartForAnUnknownProvider() {
        // given
        auth = new AdminAuthConfig("oidc", null, Map.of());

        // when / then
        assertThatThrownBy(this::startServer)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("no authentication provider 'oidc' for admin.auth.type (available: none, oauth)");
    }

    @Test
    void failsToStartWhenTheProviderRejectsItsSettings() {
        // given
        authenticationProviders = registryOf(new BuiltInAuthenticationPlugin(), new ApiKeyPlugin());
        auth = new AdminAuthConfig("test-api-key", null, Map.of());

        // when / then
        assertThatThrownBy(this::startServer)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("admin.auth.settings.apiKey is required");
    }

    private void useApiKeyProvider() {
        authenticationProviders = registryOf(new BuiltInAuthenticationPlugin(), new ApiKeyPlugin());
        auth = new AdminAuthConfig("test-api-key", null, Map.of("apiKey", "letmein"));
    }

    /// A third-party plugin: admits requests carrying the configured `X-Api-Key` and adds a public endpoint.
    static final class ApiKeyPlugin implements KawaPlugin {

        @Override
        public void register(KawaPluginContext context) {
            context.registerAuthenticationProvider(new AuthenticationProviderFactory() {
                @Override
                public String name() {
                    return "test-api-key";
                }

                @Override
                public AuthenticationProvider create(AuthenticationProviderContext providerContext) {
                    String apiKey = providerContext.settings().requireString("apiKey");
                    providerContext.publicEndpoint("GET", "/test/hello",
                            request -> HttpResponse.json(200, "{\"hello\":\"from the api-key plugin\"}"));
                    return request -> request.header("x-api-key")
                            .filter(apiKey::equals)
                            .map(_ -> new KawaPrincipal("api-key-client"));
                }
            });
        }
    }
}
