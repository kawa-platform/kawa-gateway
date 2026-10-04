package io.jonasg.kawa.plugin.auth;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/// The parts of an admin HTTP request an [AuthenticationProvider] may inspect.
///
/// @param method  the HTTP method, e.g. `GET`
/// @param path    the request path without the query string
/// @param headers the request headers, keyed by lower-case header name
public record AuthenticationRequest(String method, String path, Map<String, List<String>> headers) {

    public AuthenticationRequest {
        headers = headers.entrySet().stream().collect(Collectors.toUnmodifiableMap(
                entry -> entry.getKey().toLowerCase(Locale.ROOT),
                entry -> List.copyOf(entry.getValue()),
                (first, second) -> {
                    var merged = new ArrayList<>(first);
                    merged.addAll(second);
                    return List.copyOf(merged);
                }));
    }

    /// The first value of the header `name` (case-insensitive), if present.
    public Optional<String> header(String name) {
        List<String> values = headers.get(name.toLowerCase(Locale.ROOT));
        return values == null || values.isEmpty() ? Optional.empty() : Optional.of(values.getFirst());
    }
}
