package io.jonasg.kawa.server.plugin;

import com.example.lib.Greeting;
import io.jonasg.kawa.plugin.KawaPlugin;
import io.jonasg.kawa.plugin.auth.AdminUserDirectory;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
import io.jonasg.kawa.plugin.auth.AuthenticationRequest;
import io.jonasg.kawa.plugin.auth.KawaPrincipal;
import io.jonasg.kawa.plugin.auth.OAuthClient;
import io.jonasg.kawa.plugin.auth.OAuthClientDirectory;
import io.jonasg.kawa.plugin.auth.ProviderSettings;
import io.jonasg.kawa.plugin.http.HttpEndpoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Plugins installed in the plugin directory are loaded in isolation: each sees the JDK, the plugin API,
/// slf4j and its own JARs, never kawa's classpath.
class ExternalPluginLoaderTest {

    /// Registers a provider named `<what Greeting says>-<isolated|leaky>` whose principal, and the realm
    /// of whose challenge, is the name of the thread context classloader it was called with.
    private static final String GREETING_PLUGIN = """
            package fixture;

            import io.jonasg.kawa.plugin.KawaPlugin;
            import io.jonasg.kawa.plugin.KawaPluginContext;
            import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
            import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
            import io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory;
            import io.jonasg.kawa.plugin.auth.AuthenticationRequest;
            import io.jonasg.kawa.plugin.auth.KawaPrincipal;
            import java.util.Optional;

            public final class GreetingPlugin implements KawaPlugin {
                @Override
                public void register(KawaPluginContext context) {
                    String visibility;
                    try {
                        Class.forName("io.jonasg.kawa.server.plugin.PluginLoader");
                        visibility = "leaky";
                    } catch (ClassNotFoundException e) {
                        visibility = "isolated";
                    }
                    String name = new com.example.lib.Greeting().text() + "-" + visibility;
                    context.registerAuthenticationProvider(new AuthenticationProviderFactory() {
                        @Override
                        public String name() {
                            return name;
                        }

                        @Override
                        public AuthenticationProvider create(AuthenticationProviderContext providerContext) {
                            return new AuthenticationProvider() {
                                @Override
                                public Optional<KawaPrincipal> authenticate(AuthenticationRequest request) {
                                    return Optional.of(new KawaPrincipal(contextClassLoader()));
                                }

                                @Override
                                public String challenge() {
                                    return "Bearer realm=\\"" + contextClassLoader() + "\\"";
                                }
                            };
                        }

                        private static String contextClassLoader() {
                            return Thread.currentThread().getContextClassLoader().getName();
                        }
                    });
                }
            }
            """;

    /// Registers a provider named `oauth-clients` whose principal describes the OAuth client named by the
    /// request's `x-client-id` header, as the plugin reads it from its context.
    private static final String OAUTH_CLIENTS_PLUGIN = """
            package fixture;

            import io.jonasg.kawa.plugin.KawaPlugin;
            import io.jonasg.kawa.plugin.KawaPluginContext;
            import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
            import io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory;
            import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
            import io.jonasg.kawa.plugin.auth.KawaPrincipal;

            public final class OAuthClientsPlugin implements KawaPlugin {
                @Override
                public void register(KawaPluginContext context) {
                    context.registerAuthenticationProvider(new AuthenticationProviderFactory() {
                        @Override
                        public String name() {
                            return "oauth-clients";
                        }

                        @Override
                        public AuthenticationProvider create(AuthenticationProviderContext providerContext) {
                            return request -> request.header("x-client-id")
                                    .flatMap(id -> providerContext.oauthClients().find(id))
                                    .map(client -> new KawaPrincipal(client.id() + " " + client.enabled() + " "
                                            + client.audience() + " " + client.redirectUris()));
                        }
                    });
                }
            }
            """;

    private static final String PLUGIN_GREETING = """
            package com.example.lib;

            public final class Greeting {
                public String text() {
                    return "plugin";
                }
            }
            """;

    @TempDir
    Path plugins;

    @Test
    void loadsAPluginDirectoryWithItsOwnLibraryVersionIsolatedFromKawa() throws IOException {
        // given: the plugin and its library as two JARs in one plugin directory
        Path plugin = plugins.resolve("greeting");
        PluginJars.build(plugin.resolve("greeting.jar"),
                Map.of("fixture.GreetingPlugin", GREETING_PLUGIN, "com.example.lib.Greeting", PLUGIN_GREETING),
                Set.of("com.example.lib.Greeting"), "fixture.GreetingPlugin");
        PluginJars.build(plugin.resolve("greeting-lib.jar"), Map.of("com.example.lib.Greeting", PLUGIN_GREETING),
                Set.of(), null);

        // when
        var loaded = new PluginLoader(getClass().getClassLoader(), plugins).loadAll();

        // then
        assertThat(new Greeting().text())
                .withFailMessage(() -> "kawa's own Greeting changed")
                .isEqualTo("kawa");
        assertThat(loaded.authenticationProviders().require("plugin-isolated").name())
                .withFailMessage(() -> "the plugin did not see its own Greeting or saw kawa's classes")
                .isEqualTo("plugin-isolated");
        assertThat(loaded.authenticationProviders().require("oauth").name())
                .withFailMessage(() -> "the built-in providers were not loaded next to the plugin")
                .isEqualTo("oauth");
    }

    @Test
    void sharesThePluginApiWithKawa() throws IOException {
        // given
        PluginJars.build(plugins.resolve("greeting.jar"),
                Map.of("fixture.GreetingPlugin", GREETING_PLUGIN, "com.example.lib.Greeting", PLUGIN_GREETING),
                Set.of(), "fixture.GreetingPlugin");

        // when
        var loaded = new PluginLoader(getClass().getClassLoader(), plugins).loadAll();

        // then
        KawaPlugin external = loaded.plugins().stream()
                .filter(plugin -> plugin.getClass().getName().equals("fixture.GreetingPlugin"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("the single-JAR plugin was not loaded: " + loaded.plugins()));
        assertThat(external.getClass().getClassLoader())
                .withFailMessage(() -> "the plugin was not loaded by its own classloader")
                .isInstanceOf(PluginClassLoader.class);
        assertThat(KawaPlugin.class.getClassLoader())
                .withFailMessage(() -> "the plugin API was not shared with kawa")
                .isNotInstanceOf(PluginClassLoader.class);
    }

    @Test
    void callsThePluginWithItsClassLoaderAsContextClassLoader() throws IOException {
        // given
        PluginJars.build(plugins.resolve("greeting").resolve("greeting.jar"),
                Map.of("fixture.GreetingPlugin", GREETING_PLUGIN, "com.example.lib.Greeting", PLUGIN_GREETING),
                Set.of(), "fixture.GreetingPlugin");
        var provider = new PluginLoader(getClass().getClassLoader(), plugins).loadAll()
                .authenticationProviders().require("plugin-isolated").create(noContext());

        // when
        var principal = provider.authenticate(new AuthenticationRequest("GET", "/topics", Map.of()));

        // then
        assertThat(principal.map(KawaPrincipal::name))
                .withFailMessage(() -> "the plugin did not run with its own context classloader: " + principal)
                .hasValue("kawa-plugin:greeting");
    }

    @Test
    void forwardsThePluginsChallengeCalledWithItsClassLoader() throws IOException {
        // given
        PluginJars.build(plugins.resolve("greeting").resolve("greeting.jar"),
                Map.of("fixture.GreetingPlugin", GREETING_PLUGIN, "com.example.lib.Greeting", PLUGIN_GREETING),
                Set.of(), "fixture.GreetingPlugin");
        var provider = new PluginLoader(getClass().getClassLoader(), plugins).loadAll()
                .authenticationProviders().require("plugin-isolated").create(noContext());

        // when
        String challenge = provider.challenge();

        // then
        assertThat(challenge)
                .withFailMessage(() -> "the plugin's challenge was not forwarded or not run with its context classloader: "
                        + challenge)
                .isEqualTo("Bearer realm=\"kawa-plugin:greeting\"");
    }

    @Test
    void handsTheHostsOAuthClientsToAnIsolatedPlugin() throws IOException {
        // given
        PluginJars.build(plugins.resolve("oauth-clients").resolve("oauth-clients.jar"),
                Map.of("fixture.OAuthClientsPlugin", OAUTH_CLIENTS_PLUGIN), Set.of(), "fixture.OAuthClientsPlugin");
        var claude = new OAuthClient("kawa-mcp-claude", true, List.of("http://localhost:*/callback"), OAuthClient.Audience.MCP);
        var provider = new PluginLoader(getClass().getClassLoader(), plugins).loadAll()
                .authenticationProviders().require("oauth-clients")
                .create(contextWithOAuthClients(id -> Optional.of(claude).filter(client -> client.id().equals(id))));

        // when
        var principal = provider.authenticate(new AuthenticationRequest("GET", "/topics", Map.of("x-client-id", List.of("kawa-mcp-claude"))));

        // then
        assertThat(principal.map(KawaPrincipal::name))
                .withFailMessage(() -> "the isolated plugin did not get the host's OAuth clients: " + principal)
                .hasValue("kawa-mcp-claude true MCP [http://localhost:*/callback]");
    }

    @Test
    void failsStartupNamingThePluginWhenADependencyIsMissing() throws IOException {
        // given: a plugin whose dependency com.acme.Missing was not shipped
        PluginJars.build(plugins.resolve("broken").resolve("broken.jar"), Map.of(
                "fixture.BrokenPlugin", """
                        package fixture;

                        public final class BrokenPlugin implements io.jonasg.kawa.plugin.KawaPlugin {
                            @Override
                            public void register(io.jonasg.kawa.plugin.KawaPluginContext context) {
                                new com.acme.Missing();
                            }
                        }
                        """,
                "com.acme.Missing", "package com.acme; public final class Missing { }"),
                Set.of("com.acme.Missing"), "fixture.BrokenPlugin");

        // when / then
        assertThatThrownBy(() -> new PluginLoader(getClass().getClassLoader(), plugins).loadAll())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(plugins.resolve("broken").toString())
                .hasMessageContaining("com/acme/Missing");
    }

    @Test
    void failsStartupForAPluginWithoutServiceEntry() throws IOException {
        // given
        Files.createDirectories(plugins.resolve("empty"));
        PluginJars.build(plugins.resolve("empty").resolve("lib.jar"),
                Map.of("com.example.lib.Greeting", PLUGIN_GREETING), Set.of(), null);

        // when / then
        assertThatThrownBy(() -> new PluginLoader(getClass().getClassLoader(), plugins).loadAll())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("plugin " + plugins.resolve("empty") + " contains no META-INF/services/io.jonasg.kawa.plugin.KawaPlugin entry");
    }

    private static AuthenticationProviderContext noContext() {
        return contextWithOAuthClients(_ -> {
            throw new UnsupportedOperationException();
        });
    }

    /// A context that only offers `oauthClients`; everything else throws.
    private static AuthenticationProviderContext contextWithOAuthClients(OAuthClientDirectory oauthClients) {
        return new AuthenticationProviderContext() {
            @Override
            public ProviderSettings settings() {
                throw new UnsupportedOperationException();
            }

            @Override
            public AdminUserDirectory adminUsers() {
                throw new UnsupportedOperationException();
            }

            @Override
            public OAuthClientDirectory oauthClients() {
                return oauthClients;
            }

            @Override
            public String publicUrl() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void publicEndpoint(String method, String path, HttpEndpoint endpoint) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
