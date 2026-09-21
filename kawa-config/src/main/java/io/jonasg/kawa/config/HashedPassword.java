package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/// A client password stored in a mechanism-aware, non-reversible form.
///
/// PLAIN passwords are stored as a PBKDF2-HMAC-SHA256 hash; SCRAM-SHA-256 passwords are stored as
/// the RFC 5802 verifier (salt, StoredKey, ServerKey). The encoded form is what gets persisted in
/// the config topic; plaintext never leaves the ingestion boundary.
public final class HashedPassword {

    private final String encoded;

    private HashedPassword(String encoded) {
        this.encoded = encoded;
    }

    /// Hashes a plaintext password for the given mechanism.
    public static HashedPassword fromPlaintext(Mechanism mechanism, String plaintext) {
        if (mechanism == null) {
            throw new IllegalArgumentException("mechanism must not be null");
        }
        if (plaintext == null || plaintext.isBlank()) {
            throw new IllegalArgumentException("password must not be null or blank");
        }
        return new HashedPassword(mechanism.encode(plaintext));
    }

    /// Restores a hashed password from its encoded form.
    @JsonCreator
    public static HashedPassword fromEncoded(String encoded) {
        if (encoded == null || encoded.isBlank()) {
            throw new IllegalArgumentException("encoded password must not be null or blank");
        }
        Mechanism mechanism = Mechanism.fromEncodedPrefix(prefixOf(encoded));
        mechanism.validate(encoded);
        return new HashedPassword(encoded);
    }

    /// Verifies a plaintext password against this hash.
    ///
    /// SCRAM-SHA-256 verifiers cannot be verified yet (server-side challenge-response is not
    /// implemented), so this always returns false for them.
    public boolean verify(String plaintext) {
        if (plaintext == null) {
            return false;
        }
        return Mechanism.fromEncodedPrefix(prefixOf(encoded)).verify(encoded, plaintext);
    }

    /// The encoded form, safe to persist and transport.
    @JsonValue
    public String encoded() {
        return encoded;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof HashedPassword other && encoded.equals(other.encoded);
    }

    @Override
    public int hashCode() {
        return encoded.hashCode();
    }

    private static String prefixOf(String encoded) {
        return encoded.split("\\$", -1)[0];
    }
}
