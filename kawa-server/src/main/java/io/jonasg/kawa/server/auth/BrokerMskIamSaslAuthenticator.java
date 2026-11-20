package io.jonasg.kawa.server.auth;

import io.jonasg.kawa.config.BrokerAuthConfig;
import io.netty.channel.Channel;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.regions.Region;
import software.amazon.msk.auth.iam.IAMClientCallbackHandler;
import software.amazon.msk.auth.iam.IAMLoginModule;
import software.amazon.msk.auth.iam.internals.IAMSaslClient;
import software.amazon.msk.auth.iam.internals.utils.RegionUtils;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/// The AWS_MSK_IAM first SASL payload of a broker connection: the sign-in request, signed for the
/// configured region, or for the region in the broker hostname when none is configured.
///
/// The credentials are the named profile's, or the default AWS credential chain when no profile is set.
/// [#create] fails before anything is sent when no credentials load.
final class BrokerMskIamSaslAuthenticator implements SaslInitialResponse {

    private final BrokerAuthConfig config;
    private final @Nullable String brokerHost;

    /// `brokerHost` is the host the connection was made to; `null` falls back to the connection's remote host.
    BrokerMskIamSaslAuthenticator(BrokerAuthConfig config, @Nullable String brokerHost) {
        this.config = config;
        this.brokerHost = brokerHost;
    }

    @Override
    public String mechanism() {
        return config.mechanism();
    }

    @Override
    public boolean logsRootCauseOnly() {
        return true;
    }

    @Override
    public byte[] create(Channel channel) throws SaslException {
        IAMClientCallbackHandler callbacks = credentials(config.profile());
        try {
            String host = brokerHost != null ? brokerHost : host(channel);
            Region configured = AwsMskIam.region(config);
            Region region = configured != null ? configured : RegionUtils.extractRegionFromHost(host);
            var client = new IAMSaslClient(BrokerAuthConfig.AWS_MSK_IAM, callbacks, host,
                    new AwsMskIam.RegionSigner(region));
            return client.evaluateChallenge(new byte[0]);
        } finally {
            callbacks.close();
        }
    }

    /// The callback handler that supplies the AWS credentials: the named `profile`'s only, or the
    /// default credential chain when `profile` is `null`.
    private static IAMClientCallbackHandler credentials(@Nullable String profile) {
        var handler = new IAMClientCallbackHandler();
        List<AppConfigurationEntry> entries = profile == null
                ? List.of()
                : List.of(new AppConfigurationEntry(IAMLoginModule.class.getCanonicalName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        Map.of("awsProfileName", profile, "awsAddDefaultProviders", "false")));
        handler.configure(Map.of(), BrokerAuthConfig.AWS_MSK_IAM, entries);
        return handler;
    }

    private static String host(Channel channel) {
        return channel.remoteAddress() instanceof InetSocketAddress address
                ? address.getHostString()
                : String.valueOf(channel.remoteAddress());
    }
}
