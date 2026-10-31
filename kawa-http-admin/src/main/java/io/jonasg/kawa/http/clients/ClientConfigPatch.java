package io.jonasg.kawa.http.clients;

import java.util.List;

/// A partial update for a single auth client. Fields not supplied are preserved
/// as-is; `groups`, when supplied, replaces the client's group memberships.
///
/// @param mechanism the new SASL mechanism, or `null` to keep the current one
/// @param password  the new password, or `null` to keep the current one
/// @param groups    the replacement group names, or `null` to keep current memberships
record ClientConfigPatch(
        String mechanism,
        String password,
        List<String> groups
) {

    ClientConfigRequest toRequest() {
        return new ClientConfigRequest(mechanism, password, groups);
    }
}
