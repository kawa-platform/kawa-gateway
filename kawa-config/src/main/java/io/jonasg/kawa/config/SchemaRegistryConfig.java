package io.jonasg.kawa.config;

import org.jspecify.annotations.Nullable;

import java.net.URI;
import java.net.URISyntaxException;

/// The schema registry that holds the schemas of a cluster's records, for virtual topics whose
/// values are decoded by schema (such as [AvroFormatConfig]).
///
/// @param url       the registry's base URL, `http://` or `https://`
/// @param basicAuth HTTP basic authentication, or `null` for none
/// @param tls       TLS for an `https` registry; `null` trusts the JVM's default CAs. Not allowed
///                  for an `http` URL
public record SchemaRegistryConfig(
        String url,
        @Nullable SchemaRegistryBasicAuthConfig basicAuth,
        @Nullable TlsConfig tls
) {

    public SchemaRegistryConfig {
        String scheme = scheme(url);
        if (tls != null && scheme.equals("http")) {
            throw new IllegalArgumentException("schemaRegistry.tls is set but the url '" + url + "' is http; use https");
        }
    }

    private static String scheme(String url) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("schemaRegistry.url is required");
        }
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("schemaRegistry.url '" + url + "' is not a valid URL: " + e.getMessage(), e);
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase();
        if (!(scheme.equals("http") || scheme.equals("https")) || uri.getHost() == null) {
            throw new IllegalArgumentException("schemaRegistry.url '" + url + "' must be an absolute http:// or https:// URL");
        }
        return scheme;
    }
}
