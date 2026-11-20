package io.jonasg.kawa.config;

import org.jspecify.annotations.Nullable;

import java.util.function.Function;

/// The SASL sign-in the gateway uses towards a cluster's brokers.
///
/// With [#AWS_MSK_IAM] the sign-in is AWS IAM: no username or password is needed, `region` (when
/// absent, derived from the broker hostname) is the region the request is signed for, and `profile`
/// (when absent, the AWS default credential chain) names the credentials to sign with.
///
/// @param mechanism the SASL mechanism, e.g. `PLAIN`, `SCRAM-SHA-256` or `AWS_MSK_IAM`
/// @param username  the SASL user, required unless the mechanism is `AWS_MSK_IAM`
/// @param password  the SASL password, required unless the mechanism is `AWS_MSK_IAM`
/// @param region    the AWS region to sign for, or `null` to derive it from the broker hostname
/// @param profile   the AWS profile whose credentials are used, or `null` for the default chain
public record BrokerAuthConfig(
        String mechanism,
        @Nullable String username,
        @Nullable String password,
        @Nullable String region,
        @Nullable String profile
) {

    /// The SASL mechanism that signs with AWS IAM (Amazon MSK IAM access control).
    public static final String AWS_MSK_IAM = "AWS_MSK_IAM";

    public BrokerAuthConfig(String mechanism, String username, String password) {
        this(mechanism, username, password, null, null);
    }

    public BrokerAuthConfig {
        if (mechanism == null || mechanism.isBlank()) {
            throw new IllegalArgumentException("brokerAuth mechanism must not be null or blank");
        }
        if (!AWS_MSK_IAM.equals(mechanism)) {
            if (username == null || username.isBlank()) {
                throw new IllegalArgumentException("brokerAuth username must not be null or blank");
            }
            if (password == null || password.isBlank()) {
                throw new IllegalArgumentException("brokerAuth password must not be null or blank");
            }
        }
        if (region != null && region.isBlank()) {
            throw new IllegalArgumentException("brokerAuth region must not be blank");
        }
        if (profile != null && profile.isBlank()) {
            throw new IllegalArgumentException("brokerAuth profile must not be blank");
        }
        if (password != null) {
            password = ClientConfig.resolveEnvVars(password, System::getenv);
        }
    }

    /// Whether the sign-in is AWS IAM rather than a username and password.
    public boolean isAwsMskIam() {
        return AWS_MSK_IAM.equals(mechanism);
    }

    static BrokerAuthConfig of(
            String mechanism,
            String username,
            String password,
            Function<String, String> envLookup
    ) {
        return new BrokerAuthConfig(mechanism, username,
                password == null ? null : ClientConfig.resolveEnvVars(password, envLookup), null, null);
    }
}
