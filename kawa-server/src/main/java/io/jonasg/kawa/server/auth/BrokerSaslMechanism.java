package io.jonasg.kawa.server.auth;

import io.jonasg.kawa.config.BrokerAuthConfig;

/// Generates the mechanism-specific wire values for an upstream broker SASL exchange.
public interface BrokerSaslMechanism {

    String name();

    byte[] authBytes(String brokerHost);

    static BrokerSaslMechanism from(BrokerAuthConfig config) {
        return switch (config.mechanism()) {
            case "PLAIN" -> new PlainSaslMechanism(config);
            case "AWS_MSK_IAM" -> new IamSaslMechanism(config);
            default -> throw new IllegalArgumentException(
                    "Unsupported broker SASL mechanism: " + config.mechanism());
        };
    }
}
