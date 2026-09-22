package io.jonasg.kawa.server.auth;

import io.jonasg.kawa.config.HashedPassword;
import io.jonasg.kawa.config.Mechanism;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Bridges kawa's persisted SCRAM verifier (the `scram-sha256$iterations$salt$storedKey$serverKey`
/// encoded form) to Kafka's [ScramCredential] so the gateway can hand it to `ScramSaslServer`.
class ScramCredentialsTest {

    @Test
    void convertsSha256VerifierToScramCredential() {
        // given
        var password = HashedPassword.fromPlaintext(Mechanism.SCRAM_SHA_256, "secret");

        // when
        ScramCredential credential = ScramCredentials.toScramCredential(password);

        // then
        assertThat(credential.iterations()).isEqualTo(4096);
        assertThat(credential.salt()).hasSize(16);
        assertThat(credential.storedKey()).hasSize(32);
        assertThat(credential.serverKey()).hasSize(32);
    }

    @Test
    void convertsSha512VerifierToScramCredential() {
        // given
        var password = HashedPassword.fromPlaintext(Mechanism.SCRAM_SHA_512, "secret");

        // when
        ScramCredential credential = ScramCredentials.toScramCredential(password);

        // then
        assertThat(credential.iterations()).isEqualTo(4096);
        assertThat(credential.salt()).hasSize(16);
        assertThat(credential.storedKey()).hasSize(64);
        assertThat(credential.serverKey()).hasSize(64);
    }

    @Test
    void rejectsNonScramVerifier() {
        // given
        var password = HashedPassword.fromPlaintext(Mechanism.PLAIN, "secret");

        // when / then
        assertThatThrownBy(() -> ScramCredentials.toScramCredential(password))
                .isInstanceOf(IllegalArgumentException.class)
                .withFailMessage("a PLAIN pbkdf2 verifier must not be fed to the SCRAM bridge");
    }
}
