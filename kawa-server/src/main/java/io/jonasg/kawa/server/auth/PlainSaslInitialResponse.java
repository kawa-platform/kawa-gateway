package io.jonasg.kawa.server.auth;

import io.jonasg.kawa.config.BrokerAuthConfig;
import io.netty.channel.Channel;

import java.nio.charset.StandardCharsets;

/// The PLAIN first SASL payload: `\0user\0password`.
final class PlainSaslInitialResponse implements SaslInitialResponse {

    private final BrokerAuthConfig config;

    PlainSaslInitialResponse(BrokerAuthConfig config) {
        this.config = config;
    }

    @Override
    public String mechanism() {
        return config.mechanism();
    }

    @Override
    public byte[] create(Channel channel) {
        return ("\0" + config.username() + "\0" + config.password()).getBytes(StandardCharsets.UTF_8);
    }
}
