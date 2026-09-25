package io.jonasg.kawa.config;

import java.util.Properties;

final class PlainBrokerAuthMechanism implements BrokerAuthMechanism {

    @Override
    public String name() {
        return "PLAIN";
    }

    @Override
    public void validate(BrokerAuthConfig config) {
        if (config.username() == null || config.username().isBlank()
                || config.password() == null || config.password().isBlank()) {
            throw new IllegalArgumentException("PLAIN brokerAuth requires username and password");
        }
    }

    @Override
    public Properties kafkaClientProperties(BrokerAuthConfig config) {
        validate(config);
        var properties = new Properties();
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", name());
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"" + config.username() + "\" password=\"" + config.password() + "\";");
        return properties;
    }
}
