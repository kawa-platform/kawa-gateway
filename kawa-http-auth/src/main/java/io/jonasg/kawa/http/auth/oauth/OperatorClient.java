package io.jonasg.kawa.http.auth.oauth;

import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/// The kawa operator, the one confidential OAuth client ([TokenIssuer#OPERATOR_CLIENT_ID]) that obtains
/// admin tokens for itself with the `client_credentials` grant, as opposed to admin users who log in
/// through kawa studio. Enabled by `admin.auth.settings.operatorSecret`; without it no token is issued to
/// the operator and none is accepted.
///
/// The operator is not an admin user: its tokens name it in `sub` and `client_id`, and stop being
/// accepted as soon as it is removed from the configuration.
///
/// It may also **act as** other principals (`admin.auth.settings.operatorActsAs`): it exchanges its token
/// for one that acts as a principal matching the pattern, such as `k8s:team-a` for the VirtualTopics of
/// namespace `team-a`. A pattern is an exact principal or a prefix followed by `*`, and its fixed part
/// must contain a `:`, so no pattern can match an admin user (an email address) or the operator itself.
final class OperatorClient {

    static final String DEFAULT_ACTS_AS = "k8s:*";

    private final byte @Nullable [] secretDigest;
    private final String actsAs;

    private OperatorClient(byte @Nullable [] secretDigest, String actsAs) {
        this.secretDigest = secretDigest;
        this.actsAs = actsAs;
    }

    /// The operator with `secret` (disabled when `null`), which may act as the principals matching
    /// `actsAs`. Rejects a blank secret or a pattern that could match something other than a delegated principal.
    static OperatorClient from(@Nullable String secret, String actsAs) {
        String fixed = actsAs.endsWith("*") ? actsAs.substring(0, actsAs.length() - 1) : actsAs;
        if (fixed.isBlank() || !fixed.contains(":") || fixed.contains("*")) {
            throw new IllegalArgumentException("admin.auth.settings.operatorActsAs '" + actsAs
                    + "' must be a principal or a prefix ending in '*' with a ':' in it, e.g. 'k8s:*'");
        }
        if (secret == null) {
            return new OperatorClient(null, actsAs);
        }
        if (secret.isBlank()) {
            throw new IllegalArgumentException("admin.auth.settings.operatorSecret must not be blank");
        }
        return new OperatorClient(sha256(secret), actsAs);
    }

    /// Whether the operator is configured at all.
    boolean enabled() {
        return secretDigest != null;
    }

    /// Whether `clientId` is the operator and `secret` its secret. Compares digests in constant time, so
    /// response times reveal neither the secret nor its length.
    boolean authenticate(String clientId, String secret) {
        byte[] expected = secretDigest;
        byte[] actual = sha256(secret);
        return expected != null && TokenIssuer.OPERATOR_CLIENT_ID.equals(clientId) && MessageDigest.isEqual(expected, actual);
    }

    /// Whether the operator may (still) act as `principal`.
    boolean mayActAs(String principal) {
        if (!enabled() || principal.contains("@")) {
            return false;
        }
        return actsAs.endsWith("*")
                ? principal.length() > actsAs.length() - 1 && principal.startsWith(actsAs.substring(0, actsAs.length() - 1))
                : principal.equals(actsAs);
    }

    private static byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
