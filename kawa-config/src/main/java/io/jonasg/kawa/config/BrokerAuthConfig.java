package io.jonasg.kawa.config;

import java.util.function.Function;

public record BrokerAuthConfig(
        String mechanism,
        String username,
        String password,
        String region,
        String profile
) {

    public BrokerAuthConfig(String mechanism, String username, String password) {
        this(mechanism, username, password, null, null);
    }

    public BrokerAuthConfig {
        if (mechanism == null || mechanism.isBlank()) {
            throw new IllegalArgumentException("brokerAuth mechanism must not be null or blank");
        }
        if (!"AWS_MSK_IAM".equals(mechanism)) {
            if (username == null || username.isBlank()) {
                throw new IllegalArgumentException("brokerAuth username must not be null or blank");
            }
            if (password == null || password.isBlank()) {
                throw new IllegalArgumentException("brokerAuth password must not be null or blank");
            }
        }
        if (password != null) {
            password = ClientConfig.resolveEnvVars(password, System::getenv);
        }
    }

    static BrokerAuthConfig of(
        String mechanism,
        String username,
        String password,
        Function<String, String> envLookup
    ) {
        if (mechanism == null || mechanism.isBlank()) {
            throw new IllegalArgumentException("brokerAuth mechanism must not be null or blank");
        }
        if (!"AWS_MSK_IAM".equals(mechanism)) {
            if (username == null || username.isBlank()) {
                throw new IllegalArgumentException("brokerAuth username must not be null or blank");
            }
            if (password == null || password.isBlank()) {
                throw new IllegalArgumentException("brokerAuth password must not be null or blank");
            }
        }
        String resolvedPassword = password == null ? null : ClientConfig.resolveEnvVars(password, envLookup);
        return new BrokerAuthConfig(mechanism, username, resolvedPassword, null, null);
    }
}
