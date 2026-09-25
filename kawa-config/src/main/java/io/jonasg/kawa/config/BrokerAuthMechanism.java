package io.jonasg.kawa.config;

import java.util.Properties;

/// Mechanism-specific configuration for a gateway connection to an upstream broker.
public interface BrokerAuthMechanism {

    String name();

    void validate(BrokerAuthConfig config);

    Properties kafkaClientProperties(BrokerAuthConfig config);
}
