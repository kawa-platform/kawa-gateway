package io.jonasg.kawa.http.authentication;

import io.jonasg.kawa.config.AdminAuthConfig;
import io.jonasg.kawa.http.AdminHttpSliceTestBase;
import io.jonasg.kawa.http.authentication.builtin.BuiltInAuthenticationPlugin;
import io.jonasg.kawa.plugin.KawaPlugin;
import io.jonasg.kawa.plugin.KawaPluginContext;
import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory;
import io.jonasg.kawa.plugin.auth.AuthenticationRequest;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import io.jonasg.kawa.plugin.http.HttpResponse;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

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
    void answersA401WithTheDefaultBearerChallenge() throws Exception {
        // given
        useApiKeyProvider();
        startServer();

        // when
        var response = send("GET", "/topics");

        // then
        assertThat(response).hasStatusCode(401);
        assertThat(response.headers().firstValue("WWW-Authenticate"))
                .withFailMessage(() -> "the 401 does not carry the default bearer challenge: " + response.headers())
                .hasValue("Bearer realm=\"kawa\"");
    }

    @Test
    void answersA401WithTheProvidersOwnChallenge() throws Exception {
        // given
        authenticationProviders = registryOf(new BuiltInAuthenticationPlugin(), new ChallengingPlugin());
        auth = new AdminAuthConfig("test-challenging", null, Map.of());
        startServer();

        // when
        var response = send("GET", "/topics");

        // then
        assertThat(response).hasStatusCode(401);
        assertThat(response.headers().firstValue("WWW-Authenticate"))
                .withFailMessage(() -> "the 401 does not carry the provider's challenge: " + response.headers())
                .hasValue("Bearer realm=\"test\", error=\"invalid_token\"");
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
    void handsTheConfiguredPublicUrlToTheProvider() throws Exception {
        // given
        useApiKeyProvider();
        publicUrl = "https://kawa.example.com/";
        startServer();

        // when
        var response = send("GET", "/test/public-url");

        // then
        assertThat(response).hasStatusCode(200);
        assertThatJson(response.body())
                .withFailMessage(() -> "the provider was not handed admin.publicUrl: " + response.body())
                .isEqualTo("""
                        {"publicUrl":"https://kawa.example.com"}
                        """);
    }

    @Test
    void letsTheProviderReadTheOAuthClientsManagedThroughTheApi() throws Exception {
        // given
        useApiKeyProvider();
        startServer();
        var apiKey = Map.of("X-Api-Key", "letmein");

        // when
        var beforePut = send("GET", "/test/oauth-clients/kawa-mcp-claude");
        send("PUT", "/admin/oauth-clients/kawa-mcp-claude?consistency=applied", """
                {"redirectUris": ["http://localhost:*/callback"], "audience": "mcp"}
                """, apiKey);
        var afterPut = send("GET", "/test/oauth-clients/kawa-mcp-claude");
        send("DELETE", "/admin/oauth-clients/kawa-mcp-claude?consistency=applied", null, apiKey);
        var afterDelete = send("GET", "/test/oauth-clients/kawa-mcp-claude");

        // then
        assertThat(beforePut).hasStatusCode(404);
        assertThat(afterPut).hasStatusCode(200);
        assertThatJson(afterPut.body())
                .withFailMessage(() -> "the provider did not see the OAuth client stored through the API: " + afterPut.body())
                .isEqualTo("""
                        {"client":"kawa-mcp-claude true MCP [http://localhost:*/callback]"}
                        """);
        assertThat(afterDelete).hasStatusCode(404);
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

    /// A third-party plugin: admits requests carrying the configured `X-Api-Key` and adds public endpoints, one of
    /// which shows the OAuth client `kawa-mcp-claude` as the provider reads it from its context.
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
                    providerContext.publicEndpoint("GET", "/test/public-url",
                            request -> HttpResponse.json(200, "{\"publicUrl\":\"" + providerContext.publicUrl() + "\"}"));
                    providerContext.publicEndpoint("GET", "/test/oauth-clients/kawa-mcp-claude",
                            request -> providerContext.oauthClients().find("kawa-mcp-claude")
                                    .map(client -> HttpResponse.json(200, "{\"client\":\"" + client.id() + " " + client.enabled()
                                            + " " + client.audience() + " " + client.redirectUris() + "\"}"))
                                    .orElseGet(() -> HttpResponse.json(404, "{}")));
                    return request -> request.header("x-api-key")
                            .filter(apiKey::equals)
                            .map(_ -> new KawaPrincipal("api-key-client"));
                }
            });
        }
    }

    /// A plugin whose provider rejects every request and answers with its own challenge.
    static final class ChallengingPlugin implements KawaPlugin {

        @Override
        public void register(KawaPluginContext context) {
            context.registerAuthenticationProvider(new AuthenticationProviderFactory() {
                @Override
                public String name() {
                    return "test-challenging";
                }

                @Override
                public AuthenticationProvider create(AuthenticationProviderContext providerContext) {
                    return new AuthenticationProvider() {
                        @Override
                        public Optional<KawaPrincipal> authenticate(AuthenticationRequest request) {
                            return Optional.empty();
                        }

                        @Override
                        public String challenge() {
                            return "Bearer realm=\"test\", error=\"invalid_token\"";
                        }
                    };
                }
            });
        }
    }
}
