package io.jonasg.kawa.config;

import java.util.Properties;

final class IamBrokerAuthMechanism implements BrokerAuthMechanism {

    @Override
    public String name() {
        return "AWS_MSK_IAM";
    }

    @Override
    public void validate(BrokerAuthConfig config) {
        // IAM credentials come from the AWS provider chain or the optional profile.
    }

    @Override
    public Properties kafkaClientProperties(BrokerAuthConfig config) {
        validate(config);
        var properties = new Properties();
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", name());
        properties.put("sasl.client.callback.handler.class",
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        var jaas = new StringBuilder("software.amazon.msk.auth.iam.IAMLoginModule required");
        if (config.region() != null && !config.region().isBlank()) {
            jaas.append(" awsRegion=\"").append(config.region()).append("\"");
        }
        if (config.profile() != null && !config.profile().isBlank()) {
            jaas.append(" awsProfileName=\"").append(config.profile()).append("\"");
        }
        jaas.append(';');
        properties.put("sasl.jaas.config", jaas.toString());
        return properties;
    }
}
