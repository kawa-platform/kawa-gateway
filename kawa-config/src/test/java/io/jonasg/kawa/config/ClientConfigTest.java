package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClientConfigTest {

    private static final Function<String, String> ENV = Map.of(
            "MY_SECRET", "s3cret-from-env",
            "MY_PORT", "5432"
    )::get;

    private static final Function<String, String> EMPTY_ENV = k -> null;

    @Test
    void allowsMissingMechanismForGlobalInheritance() {
        // given
        ClientConfig config = new ClientConfig(null, HashedPassword.fromPlaintext(Mechanism.PLAIN, "secret"));

        // then
        assertThat(config.mechanism()).isNull();
        assertThat(config.password().verify("secret"))
                .withFailMessage(() -> "Hashed password did not verify against the original plaintext")
                .isTrue();
    }

    @Test
    void rejectsNullPassword() {
        assertThatThrownBy(() -> new ClientConfig("PLAIN", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("password");
    }

    @Test
    void hashesPlaintextPasswordAtConstructionBoundary() {
        // when
        ClientConfig config = ClientConfig.fromPlaintext("PLAIN", "secret");

        // then
        assertThat(config.password().encoded())
                .withFailMessage(() -> "Client configuration retained plaintext password")
                .doesNotContain("secret");
        assertThat(config.password().verify("secret"))
                .withFailMessage(() -> "Client configuration hash did not verify plaintext password")
                .isTrue();
    }

    @Test
    void constructorStoresPasswordAsIsWithoutEnvResolution() {
        // given
        HashedPassword hashed = HashedPassword.fromPlaintext(Mechanism.PLAIN, "${MY_SECRET}");

        // when
        ClientConfig config = new ClientConfig("PLAIN", hashed);

        // then
        assertThat(config.password().verify("${MY_SECRET}"))
                .withFailMessage(() -> "Constructor must not resolve environment variables; hashing happens before construction")
                .isTrue();
    }

    @Test
    void resolvesEnvironmentVariable() {
        // when
        String resolved = ClientConfig.resolveEnvVars("${MY_SECRET}", ENV);

        // then
        assertThat(resolved).isEqualTo("s3cret-from-env");
    }

    @Test
    void resolvesDefaultWhenEnvVarIsMissing() {
        // when
        String resolved = ClientConfig.resolveEnvVars("${NONEXISTENT:-fallback}", EMPTY_ENV);

        // then
        assertThat(resolved).isEqualTo("fallback");
    }

    @Test
    void rejectsUnresolvedEnvVarWithoutDefault() {
        assertThatThrownBy(() -> ClientConfig.resolveEnvVars("${NONEXISTENT}", EMPTY_ENV))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NONEXISTENT");
    }

    @Test
    void passesLiteralPasswordThroughUnchanged() {
        // when
        String resolved = ClientConfig.resolveEnvVars("plain-password", EMPTY_ENV);

        // then
        assertThat(resolved).isEqualTo("plain-password");
    }
}
