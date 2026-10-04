package io.jonasg.kawa.http.auth.oauth;

import io.jonasg.kawa.test.TestSigningKeys;
import org.junit.jupiter.api.Test;

import static io.jonasg.kawa.test.KawaAssertions.assertThat;
import static io.jonasg.kawa.test.KawaAssertions.assertThatThrownBy;

class SigningKeysTest {

    @Test
    void loadsPkcs8Key() {
        // when
        var keys = SigningKeys.from(TestSigningKeys.PKCS8_P256);

        // then
        assertThat(keys.signingKey().isPrivate())
                .withFailMessage(() -> "the PKCS#8 key was not loaded as a private key")
                .isTrue();
        assertThat(keys.signingKey().getKeyID())
                .withFailMessage(() -> "the signing key has no key id")
                .isNotBlank();
    }

    @Test
    void loadsSec1Key() {
        // when
        var keys = SigningKeys.from(TestSigningKeys.SEC1_P256);

        // then
        assertThat(keys.signingKey().isPrivate())
                .withFailMessage(() -> "the SEC1 key was not loaded as a private key")
                .isTrue();
    }

    @Test
    void publishesOnlyThePublicKey() {
        // given
        var keys = SigningKeys.from(TestSigningKeys.SEC1_P256);

        // when
        var jwks = keys.publicJwkSet();

        // then
        assertThat(jwks.getKeys()).singleElement()
                .satisfies(jwk -> assertThat(jwk.isPrivate())
                        .withFailMessage(() -> "the published JWKS contains the private key")
                        .isFalse());
    }

    @Test
    void generatesAKeyWhenNoneIsConfigured() {
        // when
        var keys = SigningKeys.from(null);

        // then
        assertThat(keys.signingKey().isPrivate())
                .withFailMessage(() -> "no private signing key was generated")
                .isTrue();
    }

    @Test
    void rejectsKeyWithoutEmbeddedPublicKey() {
        assertThatThrownBy(() -> SigningKeys.from(TestSigningKeys.SEC1_P256_WITHOUT_PUBLIC_KEY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not embed its public key");
    }

    @Test
    void rejectsOtherCurves() {
        assertThatThrownBy(() -> SigningKeys.from(TestSigningKeys.SEC1_P384))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("admin.auth.settings.signingKey: not a P-256 key");
    }

    @Test
    void rejectsNonPemInput() {
        assertThatThrownBy(() -> SigningKeys.from("not a key"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("admin.auth.settings.signingKey: missing PEM header");
    }
}
