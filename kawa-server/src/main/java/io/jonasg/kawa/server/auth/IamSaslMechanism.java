package io.jonasg.kawa.server.auth;

import io.jonasg.kawa.config.BrokerAuthConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.msk.auth.iam.internals.AWS4SignedPayloadGenerator;
import software.amazon.msk.auth.iam.internals.AuthenticationRequestParams;

final class IamSaslMechanism implements BrokerSaslMechanism {

    private static final String NAME = "AWS_MSK_IAM";
    private static final String USER_AGENT = "kawa-gateway";

    private final BrokerAuthConfig config;
    private final AwsCredentialsProvider credentialsProvider;

    IamSaslMechanism(BrokerAuthConfig config) {
        this(config, provider(config));
    }

    IamSaslMechanism(BrokerAuthConfig config, AwsCredentialsProvider credentialsProvider) {
        this.config = config;
        this.credentialsProvider = credentialsProvider;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public byte[] authBytes(String brokerHost) {
        AwsCredentials credentials = credentialsProvider.resolveCredentials();
        var params = config.region() == null || config.region().isBlank()
                ? AuthenticationRequestParams.create(brokerHost, credentials, USER_AGENT)
                : AuthenticationRequestParams.create(
                        brokerHost, credentials, USER_AGENT, new FixedRegionProvider(config.region()));
        try {
            return new AWS4SignedPayloadGenerator().signedPayload(params);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to generate AWS MSK IAM authentication payload", e);
        }
    }

    private static AwsCredentialsProvider provider(BrokerAuthConfig config) {
        return config.profile() == null || config.profile().isBlank()
                ? DefaultCredentialsProvider.create()
                : ProfileCredentialsProvider.create(config.profile());
    }
}
