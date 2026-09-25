package io.jonasg.kawa.config;

import java.util.Map;

/// Registry of broker authentication mechanisms supported by the gateway.
public final class BrokerAuthMechanisms {

    private static final Map<String, BrokerAuthMechanism> MECHANISMS = Map.of(
            "PLAIN", new PlainBrokerAuthMechanism(),
            "AWS_MSK_IAM", new IamBrokerAuthMechanism());

    private BrokerAuthMechanisms() {
    }

    public static BrokerAuthMechanism resolve(String name) {
        var mechanism = MECHANISMS.get(name);
        if (mechanism == null) {
            throw new IllegalArgumentException("Unsupported broker SASL mechanism: " + name);
        }
        return mechanism;
    }
}
