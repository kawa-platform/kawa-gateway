package io.jonasg.kawa.plugin.http;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/// An admin HTTP request as seen by an [HttpEndpoint].
///
/// @param method      the HTTP method, e.g. `POST`
/// @param path        the request path without the query string
/// @param queryParams the query parameters
/// @param headers     the request headers, keyed by lower-case header name
/// @param body        the raw request body (empty when the request has none)
public record HttpRequest(
        String method,
        String path,
        Map<String, String> queryParams,
        Map<String, List<String>> headers,
        byte[] body
) {

    /// The first value of the header `name` (lower-case), if present.
    public Optional<String> header(String name) {
        List<String> values = headers.get(name);
        return values == null || values.isEmpty() ? Optional.empty() : Optional.of(values.getFirst());
    }
}
