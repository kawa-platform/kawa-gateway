package io.jonasg.kawa.server.auth;

import io.jonasg.kawa.config.HashedPassword;
import org.apache.kafka.common.security.scram.ScramCredential;

import java.util.HexFormat;

/// Bridges kawa's persisted SCRAM verifier to Kafka's [ScramCredential] so the gateway can
/// hand it to `ScramSaslServer`.
///
/// The encoded form is `scram-sha256|scram-sha512$iterations$salt$storedKey$serverKey` (all
/// byte arrays hex-encoded), produced by `Mechanism.encode` and validated by
/// `HashedPassword.fromEncoded`.
final class ScramCredentials {

    private ScramCredentials() {
    }

    static ScramCredential toScramCredential(HashedPassword password) {
        String[] parts = password.encoded().split("\\$", -1);
        if (parts.length != 5) {
            throw new IllegalArgumentException("not a SCRAM verifier: " + password.encoded());
        }
        int iterations = Integer.parseInt(parts[1]);
        byte[] salt = HexFormat.of().parseHex(parts[2]);
        byte[] storedKey = HexFormat.of().parseHex(parts[3]);
        byte[] serverKey = HexFormat.of().parseHex(parts[4]);
        return new ScramCredential(salt, storedKey, serverKey, iterations);
    }
}
