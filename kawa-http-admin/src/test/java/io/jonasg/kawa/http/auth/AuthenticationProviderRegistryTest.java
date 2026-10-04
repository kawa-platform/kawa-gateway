package io.jonasg.kawa.http.auth;

import io.jonasg.kawa.plugin.auth.AuthenticationProvider;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderContext;
import io.jonasg.kawa.plugin.auth.AuthenticationProviderFactory;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static io.jonasg.kawa.test.KawaAssertions.assertThatThrownBy;

class AuthenticationProviderRegistryTest {

    @Test
    void returnsTheFactoryRegisteredUnderAName() {
        // given
        var registry = new AuthenticationProviderRegistry();
        var factory = factory("oidc");
        registry.register(factory);

        // when
        var found = registry.require("oidc");

        // then
        assertThat(found)
                .withFailMessage(() -> "the factory registered as 'oidc' was not returned")
                .isSameAs(factory);
    }

    @Test
    void rejectsADuplicateName() {
        // given
        var registry = new AuthenticationProviderRegistry();
        registry.register(factory("oauth"));

        // when / then
        assertThatThrownBy(() -> registry.register(factory("oauth")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("authentication provider 'oauth' is registered by both");
    }

    @Test
    void namesTheAvailableProvidersForAnUnknownOne() {
        // given
        var registry = new AuthenticationProviderRegistry();
        registry.register(factory("oauth"));
        registry.register(factory("none"));

        // when / then
        assertThatThrownBy(() -> registry.require("oidc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("no authentication provider 'oidc' for admin.auth.type (available: none, oauth)");
    }

    private static AuthenticationProviderFactory factory(String name) {
        return new AuthenticationProviderFactory() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public AuthenticationProvider create(AuthenticationProviderContext context) {
                return request -> Optional.empty();
            }
        };
    }
}
