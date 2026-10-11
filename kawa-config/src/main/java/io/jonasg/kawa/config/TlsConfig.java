package io.jonasg.kawa.config;

import org.jspecify.annotations.Nullable;

/// TLS for the gateway's connections to a cluster's brokers. Present means TLS is on: SSL, or
/// SASL_SSL together with `auth.brokerAuth` (see [BrokerSecurity#protocol]).
///
/// @param truststore     the CA certificates the brokers' certificates are verified against
/// @param keystore       the gateway's own certificate and key, for brokers that require mutual
///                       TLS; `null` when they do not
/// @param verifyHostname whether a broker's certificate must name the host it was reached on
///                       (defaults to `true`)
public record TlsConfig(
        StoreConfig truststore,
        @Nullable StoreConfig keystore,
        Boolean verifyHostname
) {

    public TlsConfig {
        if (truststore == null) {
            throw new IllegalArgumentException("tls.truststore is required: it verifies the brokers' certificates");
        }
        if (verifyHostname == null) {
            verifyHostname = true;
        }
    }
}
