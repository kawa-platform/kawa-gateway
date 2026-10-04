package io.jonasg.kawa.http;

import io.jonasg.kawa.config.CorsConfig;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/// Maps the config-layer [CorsConfig] to a Netty [io.netty.handler.codec.http.cors.CorsConfig].
/// Returns `null` when CORS is disabled so the pipeline can skip the handler entirely.
///
/// A wildcard `allowedHeaders` also lists `Authorization` explicitly: per the Fetch spec,
/// `Access-Control-Allow-Headers: *` does not cover it, and a browser UI needs it to send bearer
/// tokens to an authenticated admin API.
public final class CorsConfigFactory {

    private static final String AUTHORIZATION = "Authorization";

    private CorsConfigFactory() {
    }

    public static io.netty.handler.codec.http.cors.CorsConfig from(CorsConfig config) {
        Objects.requireNonNull(config, "CORS config must not be null");

        CorsConfigBuilder builder = config.allowedOrigins().contains("*")
                ? CorsConfigBuilder.forAnyOrigin()
                : CorsConfigBuilder.forOrigins(config.allowedOrigins().toArray(String[]::new));
        if (!config.allowedMethods().isEmpty()) {
            builder.allowedRequestMethods(config.allowedMethods().stream()
                    .map(HttpMethod::valueOf)
                    .toArray(HttpMethod[]::new));
        }
        if (!config.allowedHeaders().isEmpty()) {
            builder.allowedRequestHeaders(allowedHeaders(config.allowedHeaders()).toArray(String[]::new));
        }
        if (config.allowCredentials()) {
            builder.allowCredentials();
        }
        if (config.maxAge() != null) {
            builder.maxAge(config.maxAge());
        }
        return builder.build();
    }

    private static List<String> allowedHeaders(List<String> configured) {
        boolean wildcard = configured.contains("*");
        boolean listsAuthorization = configured.stream().anyMatch(AUTHORIZATION::equalsIgnoreCase);
        if (!wildcard || listsAuthorization) {
            return configured;
        }
        var headers = new ArrayList<>(configured);
        headers.add(AUTHORIZATION);
        return headers;
    }
}
