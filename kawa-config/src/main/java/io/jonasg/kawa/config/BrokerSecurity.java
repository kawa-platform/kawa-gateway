package io.jonasg.kawa.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.jspecify.annotations.Nullable;

import java.util.Properties;

/// How the gateway connects to a cluster's brokers, derived from a cluster's `tls` and
/// `auth.brokerAuth`, so the two cannot contradict a separately configured protocol.
public final class BrokerSecurity {

    private BrokerSecurity() {
    }

    /// PLAINTEXT, SASL_PLAINTEXT, SSL or SASL_SSL, depending on which of the two is configured.
    public static SecurityProtocol protocol(@Nullable TlsConfig tls, @Nullable BrokerAuthConfig auth) {
        if (tls == null) {
            return auth == null ? SecurityProtocol.PLAINTEXT : SecurityProtocol.SASL_PLAINTEXT;
        }
        return auth == null ? SecurityProtocol.SSL : SecurityProtocol.SASL_SSL;
    }

    /// The Kafka client settings (`security.protocol`, `sasl.*`, `ssl.*`) for the gateway's own
    /// Kafka clients: the config-topic consumer and writer, and the topic admin.
    public static Properties clientProperties(@Nullable TlsConfig tls, @Nullable BrokerAuthConfig auth) {
        var props = new Properties();
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol(tls, auth).name);
        if (auth != null) {
            props.put(SaslConfigs.SASL_MECHANISM, auth.mechanism());
            props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"" + auth.username() + "\" password=\"" + auth.password() + "\";");
        }
        if (tls != null) {
            StoreConfig truststore = tls.truststore();
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore.location());
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, truststore.type().name());
            putIfPresent(props, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststore.password());
            StoreConfig keystore = tls.keystore();
            if (keystore != null) {
                props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystore.location());
                props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, keystore.type().name());
                putIfPresent(props, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystore.password());
                putIfPresent(props, SslConfigs.SSL_KEY_PASSWORD_CONFIG, keystore.keyPassword());
            }
            if (!tls.verifyHostname()) {
                props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            }
        }
        return props;
    }

    private static void putIfPresent(Properties props, String key, @Nullable String value) {
        if (value != null) {
            props.put(key, value);
        }
    }
}
