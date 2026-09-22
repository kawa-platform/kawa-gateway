package io.jonasg.kawa.http;

/// A single auth client in the admin `/auth/clients` response.
///
/// @param username  the SASL username
/// @param mechanism the SASL mechanism (e.g. PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
public record ClientView(
        String username,
        String mechanism
) {
}
