package io.jonasg.kawa.http;

import org.jspecify.annotations.NullUnmarked;

import java.util.List;

/// A client upsert request, optionally assigning the client to existing groups.
///
/// @param mechanism the SASL mechanism
/// @param password  the client password
/// @param groups    the desired group memberships, or `null` for no groups
@NullUnmarked
public record ClientConfigRequest(
        String mechanism,
        String password,
        List<String> groups
) {
}
