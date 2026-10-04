package io.jonasg.kawa.plugin.http;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/// The response of an [HttpEndpoint].
///
/// @param status      the HTTP status code
/// @param contentType the `Content-Type` of `body`
/// @param headers     extra response headers
/// @param body        the response body, written verbatim
public record HttpResponse(int status, String contentType, Map<String, String> headers, byte[] body) {

    /// A JSON response with the given status and pre-serialized JSON body.
    public static HttpResponse json(int status, String json) {
        return new HttpResponse(status, "application/json", Map.of(), json.getBytes(StandardCharsets.UTF_8));
    }

    /// A `302 Found` redirect to `location`.
    public static HttpResponse redirect(String location) {
        return new HttpResponse(302, "text/plain", Map.of("Location", location), new byte[0]);
    }

    /// A copy of this response with the header `name` set to `value`.
    public HttpResponse withHeader(String name, String value) {
        var merged = new HashMap<>(headers);
        merged.put(name, value);
        return new HttpResponse(status, contentType, Map.copyOf(merged), body);
    }
}
