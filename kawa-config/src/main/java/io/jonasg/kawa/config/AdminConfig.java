package io.jonasg.kawa.config;

import org.jspecify.annotations.Nullable;

import java.net.URI;
import java.net.URISyntaxException;

/// Admin HTTP listener configuration for the gateway's management/UI surface.
///
/// @param enabled   whether the admin HTTP server is started (`false` disables it)
/// @param host      bind address (defaults to `0.0.0.0`)
/// @param port      bind port, or `0` to bind an ephemeral port (defaults to `8080`)
/// @param cors      CORS configuration for browser-based UIs served from another host/port
///                  (`null` disables CORS entirely)
/// @param auth      admin API authentication (defaults to none)
/// @param publicUrl the absolute URL clients reach the admin API at, e.g. `https://kawa.example.com`
///                  when kawa sits behind a proxy; `null` when not set, see [#resolvedPublicUrl()].
///                  A trailing `/` is dropped
public record AdminConfig(
        boolean enabled,
        String host,
        Integer port,
        @Nullable CorsConfig cors,
        AdminAuthConfig auth,
        @Nullable String publicUrl
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
        if (publicUrl != null) {
            publicUrl = absoluteUrl(publicUrl);
        }
    }

    public AdminConfig(
            boolean enabled,
            String host,
            Integer port,
            @Nullable CorsConfig cors,
            AdminAuthConfig auth
    ) {
        this(enabled, host, port, cors, auth, null);
    }

    public AdminConfig(
            boolean enabled,
            String host,
            Integer port,
            @Nullable CorsConfig cors
    ) {
        this(enabled, host, port, cors, null, null);
    }

    public static AdminConfig of(
            boolean enabled,
            String host,
            Integer port,
            CorsConfig cors
    ) {
        return new AdminConfig(enabled, host, port, cors);
    }

    /// The URL clients reach the admin API at: [#publicUrl()] when set, otherwise
    /// `http://<host>:<port>` built from the bind address.
    public String resolvedPublicUrl() {
        if (publicUrl != null) {
            return publicUrl;
        }
        String urlHost = host.contains(":") ? "[" + host + "]" : host;
        return "http://" + urlHost + ":" + port;
    }

    private static String absoluteUrl(String value) {
        String url = value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
        try {
            URI uri = new URI(url);
            if (uri.isAbsolute() && uri.getHost() != null
                    && ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))) {
                return url;
            }
        } catch (URISyntaxException e) {
            // reported below
        }
        throw new IllegalArgumentException("admin.publicUrl must be an absolute http(s) URL: " + value);
    }
}
