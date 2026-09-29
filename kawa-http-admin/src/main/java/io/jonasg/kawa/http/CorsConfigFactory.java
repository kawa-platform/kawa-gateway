package io.jonasg.kawa.http;

import io.jonasg.kawa.config.CorsConfig;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;

import java.util.Objects;

/// Maps the config-layer [CorsConfig] to a Netty [io.netty.handler.codec.http.cors.CorsConfig].
/// Returns `null` when CORS is disabled so the pipeline can skip the handler entirely.
public final class CorsConfigFactory {

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
            builder.allowedRequestHeaders(config.allowedHeaders().toArray(String[]::new));
        }
        if (config.allowCredentials()) {
            builder.allowCredentials();
        }
        if (config.maxAge() != null) {
            builder.maxAge(config.maxAge());
        }
        return builder.build();
    }
}
