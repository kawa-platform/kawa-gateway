package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.Nullable;

/// Admin HTTP listener configuration for the gateway's management/UI surface.
///
/// @param enabled whether the admin HTTP server is started (`false` disables it)
/// @param host    bind address (defaults to `0.0.0.0`)
/// @param port    bind port, or `0` to bind an ephemeral port (defaults to `8080`)
/// @param cors    CORS configuration for browser-based UIs served from another host/port
///             (`null` disables CORS entirely)
/// @param auth    admin API authentication (defaults to none). Read from YAML but never serialized,
///             so its secrets cannot end up in the config topic
public record AdminConfig(
        boolean enabled,
        String host,
        Integer port,
        @Nullable CorsConfig cors,
        @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) AdminAuthConfig auth
) {

    public AdminConfig {
        if (host == null) {
            host = "0.0.0.0";
        }
        if (port == null) {
            port = 8080;
        }
        if (auth == null) {
            auth = AdminAuthConfig.none();
        }
    }

    public AdminConfig(
            boolean enabled,
            String host,
            Integer port,
            @Nullable CorsConfig cors
    ) {
        this(enabled, host, port, cors, null);
    }

    public static AdminConfig of(
            boolean enabled,
            String host,
            Integer port,
            CorsConfig cors
    ) {
        return new AdminConfig(enabled, host, port, cors);
    }
}
