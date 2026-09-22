package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HashedPasswordTest {

    @Test
    void verifiesCorrectPlaintextPassword() {
        // given
        HashedPassword hashed = HashedPassword.fromPlaintext(Mechanism.PLAIN, "s3cret");

        // when
        boolean matches = hashed.verify("s3cret");

        // then
        assertThat(matches)
                .withFailMessage(() -> "Correct plaintext password was not verified")
                .isTrue();
    }

    @Test
    void rejectsWrongPlaintextPassword() {
        // given
        HashedPassword hashed = HashedPassword.fromPlaintext(Mechanism.PLAIN, "s3cret");

        // when
        boolean matches = hashed.verify("wrong");

        // then
        assertThat(matches)
                .withFailMessage(() -> "Wrong plaintext password was verified")
                .isFalse();
    }

    @Test
    void roundTripsThroughEncodedForm() {
        // given
        HashedPassword original = HashedPassword.fromPlaintext(Mechanism.PLAIN, "s3cret");

        // when
        HashedPassword restored = HashedPassword.fromEncoded(original.encoded());

        // then
        assertThat(restored.verify("s3cret"))
                .withFailMessage(() -> "Password restored from encoded form did not verify")
                .isTrue();
    }

    @Test
    void encodesPlainHashWithPbkdf2Sha256PrefixAndIterations() {
        // given
        HashedPassword hashed = HashedPassword.fromPlaintext(Mechanism.PLAIN, "s3cret");

        // when
        String encoded = hashed.encoded();

        // then
        assertThat(encoded)
                .withFailMessage(() -> "Encoded form did not use the expected PBKDF2-SHA256 format")
                .startsWith("pbkdf2-sha256$600000$");
    }

    @Test
    void producesDifferentEncodingsForSamePasswordDueToRandomSalt() {
        // given
        HashedPassword first = HashedPassword.fromPlaintext(Mechanism.PLAIN, "s3cret");
        HashedPassword second = HashedPassword.fromPlaintext(Mechanism.PLAIN, "s3cret");

        // when
        String firstEncoded = first.encoded();
        String secondEncoded = second.encoded();

        // then
        assertThat(firstEncoded)
                .withFailMessage(() -> "Two hashes of the same password were identical; salt is not random")
                .isNotEqualTo(secondEncoded);
    }

    @Test
    void encodesScramVerifierAndCannotVerifyYet() {
        // given
        HashedPassword hashed = HashedPassword.fromPlaintext(Mechanism.SCRAM_SHA_256, "s3cret");

        // when
        String encoded = hashed.encoded();
        boolean matches = hashed.verify("s3cret");

        // then
        assertThat(encoded)
                .withFailMessage(() -> "SCRAM verifier did not use the expected format")
                .startsWith("scram-sha256$4096$");
        assertThat(matches)
                .withFailMessage(() -> "SCRAM verification should not be supported yet")
                .isFalse();
    }

    @Test
    void roundTripsScramVerifierThroughEncodedForm() {
        // given
        HashedPassword original = HashedPassword.fromPlaintext(Mechanism.SCRAM_SHA_256, "s3cret");

        // when
        HashedPassword restored = HashedPassword.fromEncoded(original.encoded());

        // then
        assertThat(restored.encoded())
                .withFailMessage(() -> "SCRAM verifier did not survive the encoded round-trip")
                .isEqualTo(original.encoded());
    }

    @Test
    void encodesScramSha512VerifierAndCannotVerifyYet() {
        // given
        HashedPassword hashed = HashedPassword.fromPlaintext(Mechanism.SCRAM_SHA_512, "s3cret");

        // when
        String encoded = hashed.encoded();
        boolean matches = hashed.verify("s3cret");

        // then
        assertThat(encoded)
                .withFailMessage(() -> "SCRAM-SHA-512 verifier did not use the expected format")
                .startsWith("scram-sha512$4096$");
        assertThat(matches)
                .withFailMessage(() -> "SCRAM-SHA-512 verification should not be supported yet")
                .isFalse();
    }

    @Test
    void roundTripsScramSha512VerifierThroughEncodedForm() {
        // given
        HashedPassword original = HashedPassword.fromPlaintext(Mechanism.SCRAM_SHA_512, "s3cret");

        // when
        HashedPassword restored = HashedPassword.fromEncoded(original.encoded());

        // then
        assertThat(restored.encoded())
                .withFailMessage(() -> "SCRAM-SHA-512 verifier did not survive the encoded round-trip")
                .isEqualTo(original.encoded());
    }

    @Test
    void rejectsEncodedPasswordWithUnknownAlgorithm() {
        assertThatThrownBy(() -> HashedPassword.fromEncoded("md5$1000$salt$hash"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown algorithm");
    }

    @Test
    void rejectsMalformedEncodedPassword() {
        assertThatThrownBy(() -> HashedPassword.fromEncoded("pbkdf2-sha256$600000$only-salt"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("encoded password");
    }

    @Test
    void rejectsBlankPlaintextPassword() {
        assertThatThrownBy(() -> HashedPassword.fromPlaintext(Mechanism.PLAIN, "   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("password");
    }

    @Test
    void rejectsNullPlaintextPassword() {
        assertThatThrownBy(() -> HashedPassword.fromPlaintext(Mechanism.PLAIN, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("password");
    }
}
