package io.jonasg.kawa.http;

import io.jonasg.kawa.config.ManagedBy;
import org.jspecify.annotations.Nullable;

/// A single auth client in the admin `/auth/clients` response.
///
/// @param username  the SASL username
/// @param mechanism the SASL mechanism (e.g. PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
/// @param managedBy the owner of this client, or `null` when it is unmanaged
public record ClientConfigView(
        String username,
        String mechanism,
        @Nullable ManagedBy managedBy
) {
}
