package io.jonasg.kawa.config;

import org.apache.kafka.common.security.ssl.SslPrincipalMapper;
import org.jspecify.annotations.Nullable;

/// TLS on a client-facing listener, required by the `SSL` and `SASL_SSL` security protocols.
///
/// @param keystore              the gateway's own certificate and key, presented to clients
/// @param truststore            the CA certificates client certificates are verified against;
///                              required unless [#clientAuth] is [ClientAuth#NONE]
/// @param clientAuth            whether clients are asked for a certificate (defaults to
///                              [ClientAuth#NONE])
/// @param principalMappingRules how a client certificate's subject becomes the principal on an
///                              `SSL` listener, in Kafka's `ssl.principal.mapping.rules` syntax
///                              (defaults to the subject's CN, [#DEFAULT_PRINCIPAL_MAPPING_RULES])
public record ListenerTlsConfig(
        StoreConfig keystore,
        @Nullable StoreConfig truststore,
        ClientAuth clientAuth,
        String principalMappingRules
) {

    /// Takes the CN of the certificate's subject, and the whole subject when it has no CN.
    public static final String DEFAULT_PRINCIPAL_MAPPING_RULES = "RULE:^CN=([^,]+).*$/$1/,DEFAULT";

    public ListenerTlsConfig {
        if (keystore == null) {
            throw new IllegalArgumentException("listener tls.keystore is required: it holds the certificate the gateway presents to clients");
        }
        if (clientAuth == null) {
            clientAuth = ClientAuth.NONE;
        }
        if (clientAuth != ClientAuth.NONE && truststore == null) {
            throw new IllegalArgumentException("listener tls.truststore is required when clientAuth is " + clientAuth
                    + ": it verifies the clients' certificates");
        }
        if (principalMappingRules == null || principalMappingRules.isBlank()) {
            principalMappingRules = DEFAULT_PRINCIPAL_MAPPING_RULES;
        }
        try {
            SslPrincipalMapper.fromRules(principalMappingRules);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("listener tls.principalMappingRules is invalid: " + e.getMessage(), e);
        }
    }
}
