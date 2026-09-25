package io.jonasg.kawa.server.auth;

import io.jonasg.kawa.config.BrokerAuthConfig;

import java.nio.charset.StandardCharsets;

final class PlainSaslMechanism implements BrokerSaslMechanism {

    private final BrokerAuthConfig config;

    PlainSaslMechanism(BrokerAuthConfig config) {
        this.config = config;
    }

    @Override
    public String name() {
        return "PLAIN";
    }

    @Override
    public byte[] authBytes(String brokerHost) {
        return ("\0" + config.username() + "\0" + config.password()).getBytes(StandardCharsets.UTF_8);
    }
}
