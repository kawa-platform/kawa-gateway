package io.jonasg.kawa.config;

import java.util.List;

/// CORS configuration for the admin HTTP surface, so a browser-based UI served from a
/// different host/port can call the admin API.
///
/// @param allowedOrigins   origins allowed to call the admin API; `["*"]` allows any origin
///                       (defaults to `["*"]`)
/// @param allowedMethods   HTTP methods allowed in preflight responses (defaults to
///                       `["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]`)
/// @param allowedHeaders   request headers allowed in preflight responses (defaults to `["*"]`, which
///                         kawa extends with `Authorization` because the wildcard does not cover it)
/// @param allowCredentials whether credentialed requests (cookies, auth headers) are allowed;
///                         cannot be combined with a wildcard origin (defaults to `false`)
/// @param maxAge           how long preflight results may be cached, in seconds (`null` omits the
///               `Access-Control-Max-Age` header)
public record CorsConfig(
        List<String> allowedOrigins,
        List<String> allowedMethods,
        List<String> allowedHeaders,
        Boolean allowCredentials,
        Long maxAge) {

    public CorsConfig {
        allowedOrigins = allowedOrigins == null ? List.of("*") : List.copyOf(allowedOrigins);
        allowedMethods = allowedMethods == null
                ? List.of("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS")
                : List.copyOf(allowedMethods);
        allowedHeaders = allowedHeaders == null ? List.of("*") : List.copyOf(allowedHeaders);
        allowCredentials = allowCredentials == null ? false : allowCredentials;
    }

    public static CorsConfig of(
            List<String> allowedOrigins,
            List<String> allowedMethods,
            List<String> allowedHeaders,
            Boolean allowCredentials,
            Long maxAge
    ) {
        return new CorsConfig(allowedOrigins, allowedMethods, allowedHeaders, allowCredentials, maxAge);
    }
}
