package io.jonasg.kawa.config;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.HexFormat;

/// A SASL mechanism and the credential encoding it uses.
///
/// Each constant owns how plaintext passwords are encoded into the persisted form and how an
/// encoded credential is verified, so adding a mechanism means adding a constant rather than
/// extending a switch.
public enum Mechanism {
    PLAIN("PLAIN", "pbkdf2-sha256") {
        @Override
        String encode(String plaintext) {
            byte[] salt = newSalt();
            byte[] hash = pbkdf2(plaintext.toCharArray(), salt, PLAIN_ITERATIONS, HASH_BYTES);
            return "pbkdf2-sha256$" + PLAIN_ITERATIONS + "$" + HEX.formatHex(salt) + "$" + HEX.formatHex(hash);
        }

        @Override
        boolean verify(String encoded, String plaintext) {
            String[] parts = encoded.split("\\$", -1);
            int iterations = Integer.parseInt(parts[1]);
            byte[] salt = HEX.parseHex(parts[2]);
            byte[] expected = HEX.parseHex(parts[3]);
            byte[] actual = pbkdf2(plaintext.toCharArray(), salt, iterations, HASH_BYTES);
            return MessageDigest.isEqual(expected, actual);
        }

        @Override
        void validate(String encoded) {
            String[] parts = encoded.split("\\$", -1);
            requireParts(parts, 4, encoded);
            parseIterations(parts[1], encoded);
            validateHex(parts[2], encoded);
            validateHex(parts[3], encoded);
        }
    },
    SCRAM_SHA_256("SCRAM-SHA-256", "scram-sha256") {
        @Override
        String encode(String plaintext) {
            byte[] salt = newSalt();
            byte[] saltedPassword = pbkdf2(plaintext.toCharArray(), salt, SCRAM_ITERATIONS, HASH_BYTES);
            byte[] clientKey = hmacSha256(saltedPassword, "Client Key");
            byte[] storedKey = sha256(clientKey);
            byte[] serverKey = hmacSha256(saltedPassword, "Server Key");
            return "scram-sha256$" + SCRAM_ITERATIONS + "$" + HEX.formatHex(salt) + "$"
                    + HEX.formatHex(storedKey) + "$" + HEX.formatHex(serverKey);
        }

        @Override
        boolean verify(String encoded, String plaintext) {
            return false;
        }

        @Override
        void validate(String encoded) {
            String[] parts = encoded.split("\\$", -1);
            requireParts(parts, 5, encoded);
            parseIterations(parts[1], encoded);
            validateHex(parts[2], encoded);
            validateHex(parts[3], encoded);
            validateHex(parts[4], encoded);
        }
    };

    private static final int PLAIN_ITERATIONS = 600_000;
    private static final int SCRAM_ITERATIONS = 4_096;
    private static final int SALT_BYTES = 16;
    private static final int HASH_BYTES = 32;

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final HexFormat HEX = HexFormat.of();

    private final String wireName;
    private final String encodedPrefix;

    Mechanism(String wireName, String encodedPrefix) {
        this.wireName = wireName;
        this.encodedPrefix = encodedPrefix;
    }

    /// The mechanism name as used on the wire (Kafka protocol, HTTP JSON, YAML).
    public String wireName() {
        return wireName;
    }

    /// Resolves a wire-format mechanism name, rejecting unknown values.
    public static Mechanism fromWireName(String wireName) {
        for (Mechanism mechanism : values()) {
            if (mechanism.wireName.equals(wireName)) {
                return mechanism;
            }
        }
        throw new IllegalArgumentException("unknown mechanism: " + wireName);
    }

    /// Resolves the algorithm prefix of an encoded credential, rejecting unknown values.
    static Mechanism fromEncodedPrefix(String prefix) {
        for (Mechanism mechanism : values()) {
            if (mechanism.encodedPrefix.equals(prefix)) {
                return mechanism;
            }
        }
        throw new IllegalArgumentException("encoded password has unknown algorithm: " + prefix);
    }

    /// Encodes a plaintext password into the persisted form for this mechanism.
    abstract String encode(String plaintext);

    /// Verifies a plaintext password against an encoded credential for this mechanism.
    abstract boolean verify(String encoded, String plaintext);

    /// Validates the structure of an encoded credential, failing fast on malformed data.
    abstract void validate(String encoded);

    private static byte[] newSalt() {
        byte[] salt = new byte[SALT_BYTES];
        SECURE_RANDOM.nextBytes(salt);
        return salt;
    }

    private static byte[] pbkdf2(char[] password, byte[] salt, int iterations, int length) {
        try {
            PBEKeySpec spec = new PBEKeySpec(password, salt, iterations, length * 8);
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("PBKDF2WithHmacSHA256 is not available", e);
        }
    }

    private static byte[] hmacSha256(byte[] key, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 is not available", e);
        }
    }

    private static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private static void requireParts(String[] parts, int expected, String encoded) {
        if (parts.length != expected) {
            throw new IllegalArgumentException("encoded password has malformed form: " + encoded);
        }
    }

    private static void parseIterations(String value, String encoded) {
        try {
            int iterations = Integer.parseInt(value);
            if (iterations <= 0) {
                throw new IllegalArgumentException("encoded password has invalid iterations: " + encoded);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("encoded password has invalid iterations: " + encoded, e);
        }
    }

    private static void validateHex(String value, String encoded) {
        try {
            HEX.parseHex(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("encoded password has invalid hex: " + encoded, e);
        }
    }
}
