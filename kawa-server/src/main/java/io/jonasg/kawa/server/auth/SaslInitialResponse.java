package io.jonasg.kawa.server.auth;

import io.jonasg.kawa.config.BrokerAuthConfig;
import io.netty.channel.Channel;
import org.jspecify.annotations.Nullable;

import javax.security.sasl.SaslException;

/// The first SASL authenticate payload of a broker connection, which depends on the mechanism: PLAIN
/// sends the credentials, AWS_MSK_IAM sends a signed sign-in request.
sealed interface SaslInitialResponse permits PlainSaslInitialResponse, BrokerMskIamSaslAuthenticator {

    /// The payload for the mechanism `config` selects. `brokerHost` is the host the connection was made to;
    /// it names the AWS signing region when no region is configured, and `null` falls back to the connection's
    /// remote host.
    static SaslInitialResponse of(BrokerAuthConfig config, @Nullable String brokerHost) {
        return config.isAwsMskIam()
                ? new BrokerMskIamSaslAuthenticator(config, brokerHost)
                : new PlainSaslInitialResponse(config);
    }

    /// The SASL mechanism this payload is for, named in the logs.
    String mechanism();

    /// The payload for `channel`, generated before anything is sent. Fails when it cannot be built,
    /// for example when no AWS credentials load.
    byte[] create(Channel channel) throws SaslException;

    /// Whether a failed sign-in is logged with only its root cause (a message, never a secret) instead
    /// of the full stack trace.
    default boolean logsRootCauseOnly() {
        return false;
    }
}
