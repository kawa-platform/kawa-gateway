package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.jspecify.annotations.Nullable;

/// A listener the gateway accepts client connections on.
///
/// @param host             bind address (defaults to `0.0.0.0`)
/// @param port             bind port, or `0` to bind an ephemeral port (defaults to `9092`)
/// @param securityProtocol how clients connect, as on a Kafka broker listener: `PLAINTEXT`, `SSL`,
///                         `SASL_PLAINTEXT` or `SASL_SSL` (defaults to `SASL_PLAINTEXT`). On a `SASL_*`
///                         listener a client must authenticate before anything but `ApiVersions`
/// @param tls              TLS for the `SSL` and `SASL_SSL` protocols, and only for those
///
/// The protocol and TLS are startup-only and write-only: TLS holds store passwords, and the two are
/// validated together, so neither is written to the config topic.
public record ListenerConfig(
        String host,
        Integer port,
        @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) SecurityProtocol securityProtocol,
        @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) @Nullable ListenerTlsConfig tls
) {

    public ListenerConfig {
        if (host == null) {
            host = "0.0.0.0";
        }
        if (port == null) {
            port = 9092;
        }
        if (securityProtocol == null) {
            securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        }
        boolean usesTls = usesTls(securityProtocol);
        if (usesTls && tls == null) {
            throw new IllegalArgumentException("listener " + host + ":" + port + " uses " + securityProtocol
                    + " and needs a tls block with the gateway's keystore");
        }
        if (!usesTls && tls != null) {
            throw new IllegalArgumentException("listener " + host + ":" + port + " has a tls block but uses "
                    + securityProtocol + "; use " + (usesSasl(securityProtocol) ? "SASL_SSL" : "SSL") + " for TLS");
        }
    }

    public ListenerConfig(String host, Integer port) {
        this(host, port, null, null);
    }

    public static ListenerConfig of(
            String host,
            Integer port
    ) {
        return new ListenerConfig(host, port);
    }

    /// Whether clients must authenticate with SASL on this listener.
    public boolean usesSasl() {
        return usesSasl(securityProtocol);
    }

    private static boolean usesSasl(SecurityProtocol protocol) {
        return protocol == SecurityProtocol.SASL_PLAINTEXT || protocol == SecurityProtocol.SASL_SSL;
    }

    private static boolean usesTls(SecurityProtocol protocol) {
        return protocol == SecurityProtocol.SSL || protocol == SecurityProtocol.SASL_SSL;
    }
}
